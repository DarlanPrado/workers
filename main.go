package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	baseURL      = "https://minhareceita.org/"
	outputDir    = "cnpj_data"
	itemsPerPage = 1000
	requestDelay = 0
)

var allUFs = []string{
	"ac", "al", "ap", "am", "ba", "ce", "df", "es", "go", "ma",
	"mt", "ms", "mg", "pa", "pb", "pr", "pe", "pi", "rj", "rn", "rs",
	"ro", "rr", "sc", "sp", "se", "to",
}

type APIResponse struct {
	Data   []json.RawMessage `json:"data"`
	Cursor string            `json:"cursor"`
}

func main() {
	os.Mkdir(outputDir, 0755)
	var wg sync.WaitGroup
	var totalProcessed atomic.Int64

	// Worker por UF com conexão persistente
	for _, uf := range allUFs {
		wg.Add(1)
		go func(uf string) {
			defer wg.Done()
			client := &http.Client{
				Transport: &http.Transport{
					MaxIdleConns:        10,
					MaxIdleConnsPerHost: 10,
					IdleConnTimeout:     0, // Sem timeout
				},
			}
			processUF(client, uf, &totalProcessed)
		}(uf)
	}

	wg.Wait()
	fmt.Printf("\n✅ Processo finalizado | Total: %d CNPJs\n", totalProcessed.Load())
}

func processUF(client *http.Client, uf string, counter *atomic.Int64) {
	cursor := ""
	page := 1
	filename := filepath.Join(outputDir, fmt.Sprintf("%s.json", uf))
	file, _ := os.Create(filename)
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	writer.WriteString("[\n")
	firstItem := true

	for {
		url := fmt.Sprintf("%s?uf=%s&limit=%d", baseURL, uf, itemsPerPage)
		if cursor != "" {
			url += "&cursor=" + cursor
		}

		req, _ := http.NewRequest("GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("⚠️ [%s] Erro página %d: %v\n", uf, page, err)
			time.Sleep(5 * time.Second)
			continue
		}

		var apiResp APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			resp.Body.Close()
			fmt.Printf("⚠️ [%s] Erro decodificação: %v\n", uf, err)
			break
		}
		resp.Body.Close()

		// Escreve os itens no arquivo
		for _, item := range apiResp.Data {
			if !firstItem {
				writer.WriteString(",\n")
			}
			writer.Write(item)
			firstItem = false
			counter.Add(1)
		}

		fmt.Printf("🌐 [%s] Página %-4d | Itens: %-4d | Total: %d\n",
			uf, page, len(apiResp.Data), counter.Load())

		if apiResp.Cursor == "" {
			break
		}
		cursor = apiResp.Cursor
		page++
		time.Sleep(requestDelay)
	}

	writer.WriteString("\n]")
	fmt.Printf("✅ [%s] Finalizado (%d páginas)\n", uf, page)
}