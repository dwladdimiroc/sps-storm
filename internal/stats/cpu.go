package stats

import (
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"sync"
)

func collectCPU(wg *sync.WaitGroup, duration int) string {
	appCmdCPU := "sar"
	argsCmdCPU := []string{"-u", "1"}
	dirCmdCPU := ""
	var outputCPU string

	wg.Add(1)
	go func(appCmd string, argsCmd []string, output *string, dirCmdCPU string, duration int) {
		defer wg.Done()
		*output = util.Start(appCmd, argsCmd, dirCmdCPU, duration)
	}(appCmdCPU, argsCmdCPU, &outputCPU, dirCmdCPU, duration)

	return outputCPU
}
