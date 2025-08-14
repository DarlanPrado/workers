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

// Configurações ajustáveis
const (
	apiURL           = "https://minhareceita.org/%s"
	outputFile       = "data.json"
	cnpjLength       = 14
	maxWorkers       = 10
	requestDelay     = 100 * time.Millisecond
	maxRetries       = 3
	readBufferSize   = 4 * 1024 * 1024 * 1024 // 4GB buffer de leitura
	writeBufferSize  = 1 * 1024 * 1024        // 1MB buffer de escrita
	maxIdleConns     = 100
	idleConnTimeout  = 90 * time.Second
	resultsBatchSize = 1000
)

// Estrutura para resposta da API
type APIResponse struct {
	CNPJ  string          `json:"cnpj"`
	Data  json.RawMessage `json:"data"`
	Error string          `json:"error,omitempty"`
}

// Escritor thread-safe para o arquivo de saída
type safeWriter struct {
	file          *os.File
	buffer        *bufio.Writer
	mu            sync.Mutex
	firstRecord   bool
	recordsWritten uint64
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <diretorio_arquivos_bin>")
		return
	}
	inputDir := os.Args[1]

	// Configuração otimizada do cliente HTTP
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

	// Encontrar arquivos para processar
	files, err := filepath.Glob(filepath.Join(inputDir, "cmjs_*.bin"))
	if err != nil {
		fmt.Printf("Erro ao buscar arquivos: %v\n", err)
		return
	}
	if len(files) == 0 {
		fmt.Printf("Nenhum arquivo .bin encontrado em %s\n", inputDir)
		return
	}

	// Preparar arquivo de saída
	output, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Erro ao criar arquivo de saída: %v\n", err)
		return
	}
	defer output.Close()

	writer := &safeWriter{
		file:        output,
		buffer:      bufio.NewWriterSize(output, writeBufferSize),
		firstRecord: true,
	}
	writer.buffer.WriteString("[\n")

	// Canais para coordenação
	cnpjChan := make(chan string, 100000)
	results := make(chan *APIResponse, resultsBatchSize)
	var wg sync.WaitGroup
	var totalProcessed uint64

	// Iniciar workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker(client, cnpjChan, results, &wg, i)
	}

	// Coletar resultados
	go func() {
		for res := range results {
			writer.writeResponse(res)
			atomic.AddUint64(&totalProcessed, 1)
		}
	}()

	// Processar arquivos
	start := time.Now()
	for _, file := range files {
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

	// Flush periódico para evitar uso excessivo de memória
	if w.recordsWritten%10000 == 0 {
		w.buffer.Flush()
	}
}

func (w *safeWriter) finalize() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer.WriteString("\n]")
	w.buffer.Flush()
}

func worker(client *http.Client, cnpjChan <-chan string, results chan<- *APIResponse, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()

	for cnpj := range cnpjChan {
		time.Sleep(requestDelay)

		resp := &APIResponse{CNPJ: cnpj}
		processCNPJ(client, cnpj, resp, workerID)

		results <- resp
	}
}

func processCNPJ(client *http.Client, cnpj string, resp *APIResponse, workerID int) {
	for attempt := 0; attempt < maxRetries; attempt++ {
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
				fmt.Printf("Worker %d: CNPJ %s ✅\n", workerID, cnpj)
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

func processFile(filename string, cnpjChan chan<- string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Erro ao abrir arquivo %s: %v\n", filename, err)
		return
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, readBufferSize)
	buffer := make([]byte, cnpjLength)
	var count int

	for {
		_, err := io.ReadFull(reader, buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Erro ao ler arquivo %s: %v\n", filename, err)
			break
		}

		cnpj := string(buffer)
		cnpjChan <- cnpj
		count++

		if count%100000 == 0 {
			fmt.Printf("Arquivo %s: %d CNPJs enviados\n", filename, count)
		}
	}

	fmt.Printf("📦 %s: %d CNPJs processados\n", filename, count)
}