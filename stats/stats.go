package stats

import (
	"github.com/dwladdimiroc/sps-storm/util"
	"sync"
)

func Collect(nameApp string, duration int) {
	var wg = new(sync.WaitGroup)
	statsRAM := collectRAM(wg, duration)
	statsCPU := collectCPU(wg, duration)
	collectBandwidth(wg, duration)
	wg.Wait()

	util.ParseMemory(statsRAM, nameApp)
	util.ParseCPU(statsCPU, nameApp)
}
