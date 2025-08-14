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
    cnpjLength       = 14
    maxWorkers       = 12
    requestDelay     = 100 * time.Millisecond
    maxRetries       = 3
    readBufferSize   = 3 * 1024 * 1024 * 1024 // 4GB
    writeBufferSize  = 3 * 1024 * 1024 * 1024 // 1MB
    maxIdleConns     = 100
    idleConnTimeout  = 90 * time.Second
    resultsBatchSize = 1000
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

    // Busca recursiva por arquivos .bin
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

    fmt.Printf("Encontrados %d arquivos .bin para processar:\n", len(files))
    for _, file := range files {
        fmt.Println(" -", file)
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

// ... (mantenha as outras funções exatamente como estão na versão anterior)