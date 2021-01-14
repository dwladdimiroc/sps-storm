package stats

import (
	"github.com/dwladdimiroc/sps-storm/util"
	"sync"
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
