package stats

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/mackerelio/go-osstat/memory"
	"os"
	"sync"
	"time"
)

func collectRAM(wg *sync.WaitGroup, duration int) string {
	appCmdMemory := "vmstat"
	argsCmdMemory := []string{"-n", "-S", "M", "1"}
	dirCmdMemory := ""
	var outputMemory string

	wg.Add(1)
	go func(appCmd string, argsCmd []string, output *string, dirCmdMemory string, duration int) {
		defer wg.Done()
		*output = util.Start(appCmd, argsCmd, dirCmdMemory, duration)
	}(appCmdMemory, argsCmdMemory, &outputMemory, dirCmdMemory, duration)

	return outputMemory
}

func collectMemory(duration time.Duration) []float64 {
	var memoryStats []float64
	var limit = int(duration / INTERVAL)

	for i := 0; i < limit; i++ {
		memoryStat, err := memory.Get()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return nil
		}
		memoryCurrent := float64(memoryStat.Used) / float64(util.GB)
		memoryStats = append(memoryStats, memoryCurrent)

		time.Sleep(INTERVAL)
	}

	return memoryStats
}
