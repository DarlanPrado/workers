package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type job struct {
	idx        int
	baseStart  int
	baseEnd    int
	outputPath string
}

// calculo do DV direto em bytes para evitar strings temporárias
func calcularDVBytes(cnpj12 []byte) [2]byte {
	pesos1 := [...]int{5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2}
	pesos2 := [...]int{6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2}

	calc := func(nums []byte, pesos []int) byte {
		soma := 0
		for i := 0; i < len(nums); i++ {
			soma += int(nums[i]-'0') * pesos[i]
		}
		resto := soma % 11
		if resto < 2 {
			return '0'
		}
		return byte('0' + (11 - resto))
	}

	dv1 := calc(cnpj12, pesos1[:])
	dv2 := calc(append(cnpj12, dv1), pesos2[:])

	return [2]byte{dv1, dv2}
}

func gerarBloco(j job, globalCount *uint64) error {
	f, err := os.Create(j.outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 8*1024*1024) // buffer maior
	defer w.Flush()

	total := j.baseEnd - j.baseStart + 1
	start := time.Now()
	logInterval := 500_000
	local := 0

	// buffer de 12 bytes fixos para base + matriz
	cnpj12 := make([]byte, 12)
	cnpj12[8] = '0'
	cnpj12[9] = '0'
	cnpj12[10] = '0'
	cnpj12[11] = '1'

	linha := make([]byte, 15) // 14 dígitos + \n
	linha[14] = '\n'

	fmt.Printf("🧵 worker %02d → %08d..%08d (%,d)\n", j.idx, j.baseStart, j.baseEnd, total)

	for base := j.baseStart; base <= j.baseEnd; base++ {
		// escreve base como 8 dígitos no slice
		for i := 7; i >= 0; i-- {
			cnpj12[i] = byte('0' + base%10)
			base /= 10
		}

		dv := calcularDVBytes(cnpj12)

		// junta base + matriz + dv no buffer de linha
		copy(linha[0:12], cnpj12)
		linha[12] = dv[0]
		linha[13] = dv[1]

		w.Write(linha)
		local++

		if local%logInterval == 0 {
			elapsed := time.Since(start).Seconds()
			vel := float64(local) / elapsed
			eta := float64(total-local) / vel
			fmt.Printf("🧵 worker %02d → %,d/%,d (%.2f%%) ~%.0f/s ETA: %.0fs\n",
				j.idx, local, total, (float64(local)/float64(total))*100, vel, eta)
		}
	}

	atomic.AddUint64(globalCount, uint64(local))
	fmt.Printf("✅ worker %02d → %.2fs\n", j.idx, time.Since(start).Seconds())
	return nil
}

func main() {
	startBase := flag.Int("start", 0, "base inicial")
	endBase := flag.Int("end", 99_999_999, "base final")
	blockSize := flag.Int("block", 1_000_000, "tamanho do bloco")
	workers := flag.Int("workers", 8, "quantidade de workers")
	outDir := flag.String("out", ".", "diretório de saída")
	flag.Parse()

	totalBases := (*endBase - *startBase + 1)
	fmt.Printf("🚀 %08d..%08d (%,d bases) bloco: %,d workers: %d\n",
		*startBase, *endBase, totalBases, *blockSize, *workers)

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Printf("erro ao criar saída: %v\n", err)
		return
	}

	var jobs []job
	for inicio := *startBase; inicio <= *endBase; inicio += *blockSize {
		fim := inicio + *blockSize - 1
		if fim > *endBase {
			fim = *endBase
		}
		out := fmt.Sprintf("%s/cnpjs_%08d_%08d.txt", *outDir, inicio, fim)
		jobs = append(jobs, job{idx: len(jobs) + 1, baseStart: inicio, baseEnd: fim, outputPath: out})
	}

	jobCh := make(chan job)
	var wg sync.WaitGroup
	var globalCount uint64

	// monitor global
	stopTick := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		start := time.Now()
		for {
			select {
			case <-t.C:
				done := atomic.LoadUint64(&globalCount)
				elapsed := time.Since(start).Seconds()
				if elapsed > 0 {
					vel := float64(done) / elapsed
					fmt.Printf("📊 GLOBAL: %,d (~%.0f/s)\n", done, vel)
				}
			case <-stopTick:
				return
			}
		}
	}()

	runtime.GOMAXPROCS(*workers)
	wg.Add(*workers)
	for w := 1; w <= *workers; w++ {
		go func(id int) {
			defer wg.Done()
			for j := range jobCh {
				if err := gerarBloco(j, &globalCount); err != nil {
					fmt.Printf("❌ worker %02d: %v\n", id, err)
				}
			}
		}(w)
	}

	go func() {
		for _, j := range jobs {
			jobCh <- j
		}
		close(jobCh)
	}()

	startAll := time.Now()
	wg.Wait()
	close(stopTick)
	fmt.Printf("🎉 total: %,d | tempo: %.2fs | arquivos: %d\n",
		atomic.LoadUint64(&globalCount), time.Since(startAll).Seconds(), len(jobs))
}
