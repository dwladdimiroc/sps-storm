package adaptive

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"log"
)

func monitor(topologyId string, topology *storm.Topology) bool {
	if ok, metricsApi := storm.GetMetrics(topologyId); ok {
		log.Printf("monitor: update stats topology")
		updateTopology(topology, metricsApi)
		saveMetrics(*topology)
		topology.ClearStatsTimeWindow()
		period++
		return ok
	} else {
		log.Printf("monitor: error get metric")
		return ok
	}
}

func updateTopology(topology *storm.Topology, api storm.MetricsAPI) {
	updateStatsInputStream(topology, api)
	updateStatsBolt(topology, api)
}

func updateStatsInputStream(topology *storm.Topology, api storm.MetricsAPI) {
	for _, spout := range api.Spouts {
		for _, bolt := range api.Bolts {
			for _, executed := range bolt.Executed {
				if spout.ID == executed.ComponentID {
					for _, transferred := range spout.Emitted {
						if transferred.StreamID == executed.StreamID {
							for i := range topology.Bolts {
								if bolt.ID == topology.Bolts[i].Name {
									topology.Bolts[i].Input = int64(transferred.Value)
								}
							}
						}
					}
				}
			}
		}
	}
}

func updateStatsBolt(topology *storm.Topology, api storm.MetricsAPI) {
	for _, bolt := range api.Bolts {
		updateOutputBolt(topology, bolt)
	}

	for i := range topology.Bolts {
		updateInputBolt(&topology.Bolts[i], api)
		topology.Bolts[i].CalculateStats()
	}
}

func updateOutputBolt(topology *storm.Topology, boltApi storm.BoltMetric) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == boltApi.ID {
			for _, executed := range boltApi.Executed {
				topology.Bolts[i].Output += int64(executed.Value)
				topology.Bolts[i].ExecutedTotal += topology.Bolts[i].Output
				for _, executedMsAvg := range boltApi.ExecutedMsAvg {
					if executed.ComponentID == executedMsAvg.ComponentID {
						topology.Bolts[i].ExecutedTimeAvg = (executed.Value * executedMsAvg.ValueFloat) + topology.Bolts[i].ExecutedTimeAvg
					}
				}
			}
			topology.Bolts[i].ExecutedTimeAvg = topology.Bolts[i].ExecutedTimeAvg / float64(topology.Bolts[i].Output)
		}
	}
}

func updateInputBolt(bolt *storm.Bolt, api storm.MetricsAPI) {
	var inputBolt []string
	for _, boltApi := range api.Bolts {
		if boltApi.ID == bolt.Name {
			for _, executed := range boltApi.Executed {
				inputBolt = append(inputBolt, executed.ComponentID)
			}
		}
	}

	for _, input := range inputBolt {
		for _, boltApi := range api.Bolts {
			if boltApi.ID == input {
				for _, executed := range boltApi.Executed {
					bolt.Input += int64(executed.Value)
				}
			}
		}
	}
}

func saveMetrics(topology storm.Topology) {
	for _, bolt := range topology.Bolts {
		if err := util.WriteCsv(topology.Id, bolt.Name, []storm.Bolt{bolt}); err != nil {
			fmt.Printf("error write csv: %v\n", err)
		}
	}
}
