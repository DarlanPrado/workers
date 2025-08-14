package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	apiURL           = "https://minhareceita.org/%s"
	outputFile       = "data.json"
	checkpointFile   = "checkpoint.txt"
	cnpjLength       = 14
	maxWorkers       = 16
	requestDelay     = 0
	maxRetries       = 3
	readBufferSize   = 7 * 1024 * 1024 * 1024 // 4GB
	writeBufferSize  = 1 * 1024 * 1024 * 1024 // 1GB
	maxIdleConns     = 100
	idleConnTimeout  = 90 * time.Second
	resultsBatchSize = 1000
	flushInterval    = 1000
	statsInterval    = 30 * time.Second // Intervalo para mostrar estatísticas
)

type APIResponse struct {
	CNPJ  string          `json:"cnpj"`
	Data  json.RawMessage `json:"data"`
	Error string          `json:"error,omitempty"`
}

type safeWriter struct {
	file          *os.File
	buffer        *bufio.Writer
	mu            sync.Mutex
	firstRecord   bool
	recordsWritten uint64
}

type checkpointManager struct {
	mu            sync.Mutex
	processedCNPJs map[string]bool
	currentFile   string
	lastPosition  int64
}

type workerStats struct {
	mu          sync.Mutex
	processed   int
	lastCNPJ    string
	startTime   time.Time
	lastRequest time.Duration
	avgTime     time.Duration
}

var (
	checkpoint = checkpointManager{
		processedCNPJs: make(map[string]bool),
	}
	workersStats = make(map[int]*workerStats)
	statsMutex   sync.Mutex
)

func main() {
	loadCheckpoint()

	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <diretorio_arquivos_bin>")
		return
	}
	inputDir := os.Args[1]

	if _, err := os.Stat(inputDir); os.IsNotExist(err) {
		fmt.Printf("Diretório não encontrado: %s\n", inputDir)
		return
	}

	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		IdleConnTimeout:     idleConnTimeout,
		DisableCompression:  false,
		MaxIdleConnsPerHost: maxIdleConns,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	var files []string
	err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".bin" {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Erro ao buscar arquivos: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Printf("Nenhum arquivo .bin encontrado em %s e seus subdiretórios\n", inputDir)
		return
	}

	output, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Erro ao criar arquivo de saída: %v\n", err)
		return
	}

	writer := &safeWriter{
		file:        output,
		buffer:      bufio.NewWriterSize(output, writeBufferSize),
		firstRecord: true,
	}
	writer.buffer.WriteString("[\n")

	cnpjChan := make(chan string, 100000)
	results := make(chan *APIResponse, resultsBatchSize)
	var wg sync.WaitGroup
	var totalProcessed uint64

	// Inicia goroutine para mostrar estatísticas
	go printWorkerStats()

	// Inicia workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		workersStats[i] = &workerStats{
			startTime: time.Now(),
		}
		go worker(client, cnpjChan, results, &wg, i)
	}

	go func() {
		for res := range results {
			writer.writeResponse(res)
			atomic.AddUint64(&totalProcessed, 1)
		}
	}()

	start := time.Now()
	for _, file := range files {
		if checkpoint.currentFile != "" && checkpoint.currentFile != file {
			continue
		}
		processFile(file, cnpjChan)
	}

	close(cnpjChan)
	wg.Wait()
	close(results)

	writer.finalize()

	fmt.Printf("\nProcessamento concluído!\n")
	fmt.Printf("Total de CNPJs processados: %d\n", totalProcessed)
	fmt.Printf("Tempo total: %v\n", time.Since(start))
	fmt.Printf("Taxa de processamento: %.2f CNPJs/segundo\n",
		float64(totalProcessed)/time.Since(start).Seconds())
}

// Mostra estatísticas dos workers periodicamente
func printWorkerStats() {
	for {
		time.Sleep(statsInterval)
		statsMutex.Lock()
		fmt.Println("\n--- Estatísticas dos Workers ---")
		for id, stats := range workersStats {
			stats.mu.Lock()
			uptime := time.Since(stats.startTime)
			rate := float64(stats.processed) / uptime.Seconds()
			fmt.Printf("Worker %d: %d CNPJs | Último: %s | Tempo médio: %v | Taxa: %.2f/s\n",
				id, stats.processed, stats.lastCNPJ, stats.avgTime, rate)
			stats.mu.Unlock()
		}
		statsMutex.Unlock()
	}
}

// Atualiza estatísticas do worker
func updateWorkerStats(id int, cnpj string, duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	stats := workersStats[id]
	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.processed++
	stats.lastCNPJ = cnpj
	stats.lastRequest = duration
	
	// Calcula média móvel do tempo de processamento
	if stats.avgTime == 0 {
		stats.avgTime = duration
	} else {
		stats.avgTime = (stats.avgTime*9 + duration) / 10
	}
}

func (w *safeWriter) writeResponse(res *APIResponse) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.firstRecord {
		w.buffer.WriteString(",\n")
	} else {
		w.firstRecord = false
	}

	data, _ := json.Marshal(res)
	w.buffer.Write(data)
	w.recordsWritten++

	if w.recordsWritten%flushInterval == 0 {
		w.buffer.Flush()
	}
}

func (w *safeWriter) finalize() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer.WriteString("\n]")
	w.buffer.Flush()
	w.file.Close()
}

func worker(client *http.Client, cnpjChan <-chan string, results chan<- *APIResponse, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	for cnpj := range cnpjChan {
		start := time.Now()
		time.Sleep(requestDelay)

		resp := &APIResponse{CNPJ: cnpj}
		processCNPJ(client, cnpj, resp, id)

		results <- resp
		updateWorkerStats(id, cnpj, time.Since(start))
	}
}

func processCNPJ(client *http.Client, cnpj string, resp *APIResponse, workerID int) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		reqStart := time.Now()
		url := fmt.Sprintf(apiURL, cnpj)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			resp.Error = fmt.Sprintf("Erro ao criar request: %v", err)
			continue
		}

		req.Header.Set("User-Agent", fmt.Sprintf("CNPJ-Worker-%d", workerID))
		response, err := client.Do(req)
		if err != nil {
			resp.Error = fmt.Sprintf("Erro na requisição: %v", err)
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			resp.Error = fmt.Sprintf("Erro ao ler resposta: %v", err)
			continue
		}

		if response.StatusCode == http.StatusOK {
			if json.Valid(body) {
				resp.Data = body
				resp.Error = ""
				fmt.Printf("Worker %d: CNPJ %s ✅ (tempo: %v)\n", workerID, cnpj, time.Since(reqStart))
				return
			}
			resp.Error = "Resposta JSON inválida"
		} else {
			resp.Error = fmt.Sprintf("Status %d", response.StatusCode)
		}

		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}
	fmt.Printf("Worker %d: CNPJ %s ❌ (%s)\n", workerID, cnpj, resp.Error)
}

func loadCheckpoint() {
	if _, err := os.Stat(checkpointFile); os.IsNotExist(err) {
		return
	}

	file, err := os.Open(checkpointFile)
	if err != nil {
		fmt.Printf("Erro ao abrir checkpoint: %v\n", err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&checkpoint); err != nil {
		fmt.Printf("Erro ao ler checkpoint: %v\n", err)
	}
	fmt.Printf("Checkpoint carregado: %+v\n", checkpoint)
}

func saveCheckpoint(currentFile string, position int64) {
	checkpoint.mu.Lock()
	defer checkpoint.mu.Unlock()

	checkpoint.currentFile = currentFile
	checkpoint.lastPosition = position

	file, err := os.Create(checkpointFile)
	if err != nil {
		fmt.Printf("Erro ao salvar checkpoint: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(checkpoint); err != nil {
		fmt.Printf("Erro ao codificar checkpoint: %v\n", err)
	}
}

func processFile(filename string, cnpjChan chan<- string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Erro ao abrir arquivo %s: %v\n", filename, err)
		return
	}
	defer file.Close()

	if checkpoint.currentFile == filename {
		_, err = file.Seek(checkpoint.lastPosition, 0)
		if err != nil {
			fmt.Printf("Erro ao posicionar arquivo: %v\n", err)
			return
		}
	}

	reader := bufio.NewReaderSize(file, readBufferSize)
	buffer := make([]byte, cnpjLength)
	var count int
	lastSave := time.Now()

	for {
		position, _ := file.Seek(0, io.SeekCurrent)
		_, err := io.ReadFull(reader, buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Erro ao ler arquivo %s: %v\n", filename, err)
			break
		}

		cnpj := string(buffer)
		if checkpoint.processedCNPJs[cnpj] {
			continue
		}

		cnpjChan <- cnpj
		count++

		checkpoint.processedCNPJs[cnpj] = true
		checkpoint.currentFile = filename
		checkpoint.lastPosition = position + cnpjLength

		if time.Since(lastSave) > 30*time.Second {
			saveCheckpoint(filename, position)
			lastSave = time.Now()
		}

		if count%100000 == 0 {
			fmt.Printf("Arquivo %s: %d CNPJs enviados\n", filename, count)
		}
	}

	saveCheckpoint("", 0)
	fmt.Printf("📦 %s: %d CNPJs processados\n", filename, count)
}