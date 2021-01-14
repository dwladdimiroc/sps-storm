package stats

import (
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"sync"
	"time"
)

const INTERVAL = 1 * time.Second

func Collect(nameApp string, duration int) {
	var wg = new(sync.WaitGroup)
	statsRAM := collectRAM(wg, duration)
	statsCPU := collectCPU(wg, duration)
	//CollectBandwidth(duration)
	wg.Wait()

	util.ParseMemory(statsRAM, nameApp)
	util.ParseCPU(statsCPU, nameApp)
}
