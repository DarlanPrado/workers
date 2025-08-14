package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	apiURL           = "https://minhareceita.org/%s"
	outputFile       = "data.json"
	cnpjLength       = 14
	maxWorkers       = 12
	requestDelay     = 100 * time.Millisecond
	maxRetries       = 3
	readBufferSize   = 4 * 1024 * 1024 * 1024 // 4GB
	writeBufferSize  = 1 * 1024 * 1024        // 1MB
	maxIdleConns     = 100
	idleConnTimeout  = 90 * time.Second
	resultsBatchSize = 1000
	flushInterval    = 1000 // Flush a cada 1000 registros
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

func main() {
	// Configuração para capturar Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <diretorio_arquivos_bin>")
		return
	}
	inputDir := os.Args[1]

	// Verifica se o diretório existe
	if _, err := os.Stat(inputDir); os.IsNotExist(err) {
		fmt.Printf("Diretório não encontrado: %s\n", inputDir)
		return
	}

	// Configuração do cliente HTTP
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

	// Busca por arquivos .bin
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

	// Prepara arquivo de saída
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

	// Canais para coordenação
	cnpjChan := make(chan string, 100000)
	results := make(chan *APIResponse, resultsBatchSize)
	var wg sync.WaitGroup
	var totalProcessed uint64

	// Inicia workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker(client, cnpjChan, results, &wg, i)
	}

	// Rotina para tratamento de sinais
	go func() {
		<-sigChan
		fmt.Println("\nRecebido sinal de interrupção. Finalizando...")
		close(cnpjChan)
	}()

	// Coletor de resultados
	go func() {
		for res := range results {
			writer.writeResponse(res)
			atomic.AddUint64(&totalProcessed, 1)
		}
	}()

	// Processa arquivos
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

	// Flush periódico para garantir que os dados são salvos
	if w.recordsWritten%flushInterval == 0 {
		err := w.buffer.Flush()
		if err != nil {
			fmt.Printf("Erro ao fazer flush: %v\n", err)
		}
	}
}

func (w *safeWriter) finalize() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer.WriteString("\n]")
	err := w.buffer.Flush()
	if err != nil {
		fmt.Printf("Erro ao finalizar arquivo: %v\n", err)
	}
	w.file.Close()
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