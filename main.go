package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ------------------------- CONFIGURAÇÕES ------------------------- //
const (
	apiURL           = "https://minhareceita.org/%s"
	outputFile       = "data.json"
	checkpointFile   = "checkpoint.txt"
	cnpjLength       = 14
	maxWorkers       = 18                    // Workers para lidar com 404s
	requestDelay     = 25 * time.Millisecond // Mais agressivo
	maxRetries       = 1                     // Fail-fast para 404s
	readBufferSize   = 6 * 1024 * 1024 * 1024 // 6GB (60% da RAM)
	writeBufferSize  = 2 * 1024 * 1024       // 2MB buffer de escrita
	maxIdleConns     = 300                   // Conexões paralelas
	idleConnTimeout  = 120 * time.Second     // Keep-alive longo
	resultsBatchSize = 3000                  // Buffer grande
	flushInterval    = 500                   // Flush frequente
	statsInterval    = 10 * time.Second      // Monitoramento ágil
	negativeCacheTTL = 5 * time.Minute       // Cache para CNPJs inválidos
)

// ------------------------- ESTRUTURAS ------------------------- //
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
	successes   int
	lastCNPJ    string
	startTime   time.Time
	lastRequest time.Duration
	avgTime     time.Duration
}

type negativeCache struct {
	mu    sync.RWMutex
	items map[string]time.Time
}

// ------------------------- VARIÁVEIS GLOBAIS ------------------------- //
var (
	checkpoint = checkpointManager{
		processedCNPJs: make(map[string]bool),
	}
	workersStats   = make(map[int]*workerStats)
	statsMutex     sync.Mutex
	globalStart    time.Time
	totalCNPJs     uint64
	completedFiles int
	notFoundCache  = negativeCache{
		items: make(map[string]time.Time),
	}
)

// ------------------------- FUNÇÕES AUXILIARES ------------------------- //

// Cache negativo para CNPJs com 404
func (nc *negativeCache) add(cnpj string) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.items[cnpj] = time.Now()
}

func (nc *negativeCache) contains(cnpj string) bool {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	_, exists := nc.items[cnpj]
	return exists
}

func (nc *negativeCache) cleanup() {
	for range time.Tick(1 * time.Minute) {
		nc.mu.Lock()
		for cnpj, added := range nc.items {
			if time.Since(added) > negativeCacheTTL {
				delete(nc.items, cnpj)
			}
		}
		nc.mu.Unlock()
	}
}

// Carrega checkpoint do arquivo
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
}

// Salva checkpoint no arquivo
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

// Processa resultados e escreve no arquivo
func processResults(results <-chan *APIResponse, writer *safeWriter) {
	for res := range results {
		writer.writeResponse(res)
	}
}

// Atualiza estatísticas do worker
func updateWorkerStats(id int, cnpj string, duration time.Duration, success bool) {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	stats := workersStats[id]
	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.processed++
	if success {
		stats.successes++
		stats.avgTime = (stats.avgTime*9 + duration) / 10
	}
	stats.lastCNPJ = cnpj
	stats.lastRequest = duration
}

// ------------------------- FUNÇÕES PRINCIPAIS ------------------------- //

// Worker otimizado com cache negativo
func worker(client *http.Client, cnpjChan <-chan string, results chan<- *APIResponse, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	
	for cnpj := range cnpjChan {
		start := time.Now()
		
		// Verifica cache negativo primeiro
		if notFoundCache.contains(cnpj) {
			results <- &APIResponse{CNPJ: cnpj, Error: "404 (cached)"}
			updateWorkerStats(id, cnpj, time.Since(start), false)
			continue
		}

		time.Sleep(requestDelay)
		resp := &APIResponse{CNPJ: cnpj}
		processCNPJ(client, cnpj, resp, id)
		
		results <- resp
		updateWorkerStats(id, cnpj, time.Since(start), resp.Error == "")
	}
}

// Processamento de CNPJ com tratamento diferenciado para 404s
func processCNPJ(client *http.Client, cnpj string, resp *APIResponse, workerID int) {
	req, _ := http.NewRequest("GET", fmt.Sprintf(apiURL, cnpj), nil)
	req.Header.Set("User-Agent", fmt.Sprintf("CNPJ-Worker-%d", workerID))
	
	response, err := client.Do(req)
	if err != nil {
		resp.Error = fmt.Sprintf("Erro: %v", err)
		return
	}
	defer response.Body.Close()

	body, _ := io.ReadAll(response.Body)
	
	switch {
	case response.StatusCode == http.StatusOK && json.Valid(body):
		resp.Data = body
	case response.StatusCode == http.StatusNotFound:
		resp.Error = "404"
		notFoundCache.add(cnpj)
	default:
		resp.Error = fmt.Sprintf("Status %d", response.StatusCode)
	}
}

// Processa arquivos binários
func processFile(filename string, cnpjChan chan<- string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Erro ao abrir arquivo %s: %v\n", filename, err)
		return
	}
	defer file.Close()

	if checkpoint.currentFile == filename {
		file.Seek(checkpoint.lastPosition, 0)
	}

	reader := bufio.NewReaderSize(file, readBufferSize)
	buffer := make([]byte, cnpjLength*100000) // Blocos de ~1.4MB
	var count int

	for {
		n, err := reader.Read(buffer)
		if err != nil && err != io.EOF {
			break
		}

		for i := 0; i <= n-cnpjLength; i += cnpjLength {
			cnpj := string(buffer[i : i+cnpjLength])
			if !checkpoint.processedCNPJs[cnpj] {
				cnpjChan <- cnpj
				count++
				atomic.AddUint64(&totalCNPJs, 1)
				checkpoint.processedCNPJs[cnpj] = true
			}
		}

		if err == io.EOF {
			break
		}
	}
	saveCheckpoint("", 0)
	fmt.Printf("✓ %s processado (%d CNPJs)\n", filename, count)
	completedFiles++
}

// Escreve respostas no arquivo JSON
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

// Finaliza o arquivo JSON
func (w *safeWriter) finalize() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer.WriteString("\n]")
	w.buffer.Flush()
	w.file.Close()
}

// Mostra estatísticas periódicas
func printStats() {
	for range time.Tick(statsInterval) {
		statsMutex.Lock()
		fmt.Printf("\n=== Estatísticas (Tempo: %v) ===\n", time.Since(globalStart).Round(time.Second))
		fmt.Printf("Arquivos: %d | CNPJs: %d\n", completedFiles, atomic.LoadUint64(&totalCNPJs))
		
		for id, stats := range workersStats {
			stats.mu.Lock()
			rate := float64(stats.processed) / time.Since(stats.startTime).Seconds()
			fmt.Printf("W%02d: %6d (%.1f/s) | Sucessos: %d | Avg: %v\n",
				id, stats.processed, rate, stats.successes, stats.avgTime.Round(time.Millisecond))
			stats.mu.Unlock()
		}
		statsMutex.Unlock()
	}
}

// Mostra estatísticas finais
func printFinalStats(totalFiles int) {
	totalTime := time.Since(globalStart)
	fmt.Printf("\n=== PROCESSAMENTO CONCLUÍDO ===\n")
	fmt.Printf("Tempo Total: %v\n", totalTime.Round(time.Second))
	fmt.Printf("Arquivos Processados: %d/%d\n", completedFiles, totalFiles)
	fmt.Printf("CNPJs Processados: %d\n", atomic.LoadUint64(&totalCNPJs))
	fmt.Printf("Taxa Média: %.1f CNPJs/segundo\n", float64(atomic.LoadUint64(&totalCNPJs))/totalTime.Seconds())
}

// ------------------------- FUNÇÃO PRINCIPAL ------------------------- //
func main() {
	globalStart = time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())
	go notFoundCache.cleanup()

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
		MaxConnsPerHost:     maxIdleConns,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   8 * time.Second,
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
		fmt.Printf("Nenhum arquivo .bin encontrado em %s\n", inputDir)
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

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		workersStats[i] = &workerStats{startTime: time.Now()}
		go worker(client, cnpjChan, results, &wg, i)
	}

	go printStats()
	go processResults(results, writer)

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
	printFinalStats(len(files))
}