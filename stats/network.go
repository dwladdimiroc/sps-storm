package stats

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/util"
	"github.com/mackerelio/go-osstat/network"
	"os"
	"time"
)

type networkStat struct {
	receive  float64
	transmit float64
}

func CollectBandwidth(duration time.Duration) map[string][]networkStat {
	var networkStats = make(map[string][]networkStat)
	var limit = int(duration / INTERVAL)

	for i := 0; i < limit; i++ {
		stats, err := network.Get()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return nil
		}

		for j := range stats {
			var statCurrent = networkStat{
				receive:  float64(int(stats[j].RxBytes) / util.MB),
				transmit: float64(int(stats[j].TxBytes) / util.MB),
			}
			networkStats[stats[j].Name] = append(networkStats[stats[j].Name], statCurrent)
		}

		time.Sleep(INTERVAL)
	}

	return networkStats
}
