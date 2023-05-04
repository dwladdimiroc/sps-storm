package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
	"log"
)

func monitor(topologyId string, topology *storm.Topology) bool {
	if ok, metricsApi := storm.GetMetrics(topologyId); ok {
		log.Printf("monitor: update stats topology")
		updateTopology(topology, metricsApi)
		saveMetrics(*topology)
		period++
		if !topology.Benchmark && period == viper.GetInt("storm.adaptive.benchmark_samples") {
			topology.BenchmarkExecutedTimeAvg()
		}
		return ok
	} else {
		log.Printf("monitor: error get metric")
		return ok
	}
}

func updateTopology(topology *storm.Topology, api storm.MetricsAPI) {
	updateStatsInputStream(topology, api)
	updateCompleteLatency(topology, api)
	updateStatsBolt(topology, api)
}

func updateStatsInputStream(topology *storm.Topology, api storm.MetricsAPI) {
	var inputRate int64
	for _, spout := range api.Spouts {
		for _, emitted := range spout.Emitted {
			if emitted.StreamID != "__metrics" && emitted.StreamID != "__ack_init" && emitted.StreamID != "__system" {
				if inputRate == 0 {
					inputRate = int64(emitted.Value) - topology.InputAccum
					topology.InputAccum = int64(emitted.Value)
				}
				for i := range topology.Bolts {
					if topology.Bolts[i].Name == emitted.StreamID {
						topology.Bolts[i].Input = inputRate
					}
				}
			}
		}
	}

	topology.InputRate = append(topology.InputRate, inputRate)
	//log.Printf("[monitor] period={%d},inputRate={%d}", period, inputRate)
}

func updateCompleteLatency(topology *storm.Topology, api storm.MetricsAPI) {
	var completeLatency float64
	for _, spout := range api.Spouts {
		for _, channel := range spout.CompleteMsAvg {
			completeLatency += channel.ValueFloat
		}
	}
	for i := range topology.Bolts {
		topology.Bolts[i].CompleteLatency = completeLatency
	}
}

func updateStatsBolt(topology *storm.Topology, api storm.MetricsAPI) {
	for _, bolt := range api.Bolts {
		updateOutputBolt(topology, bolt)
		updateExecutedAvg(topology, bolt)
	}

	for i := range topology.Bolts {
		topology.Bolts[i].Time = int64(period) * viper.GetInt64("storm.adaptive.time_window_size")
		updateInputBolt(&topology.Bolts[i], api)
	}
}

func updateOutputBolt(topology *storm.Topology, boltApi storm.BoltMetric) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == boltApi.ID {
			var outputRate int64
			for _, executed := range boltApi.Executed {
				outputRate += int64(executed.Value)
			}
			topology.Bolts[i].Output = outputRate - topology.Bolts[i].ExecutedTotal
			topology.Bolts[i].ExecutedTotal = outputRate
		}
	}
}

func updateExecutedAvg(topology *storm.Topology, boltApi storm.BoltMetric) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == boltApi.ID {
			for _, executed := range boltApi.Executed {
				for _, executedMsAvg := range boltApi.ExecutedMsAvg {
					if executed.ComponentID == executedMsAvg.ComponentID {
						topology.Bolts[i].ExecutedTimeAvg = executedMsAvg.ValueFloat
					}
				}
			}
			topology.Bolts[i].ExecutedTimeAvgSamples = append(topology.Bolts[i].ExecutedTimeAvgSamples, topology.Bolts[i].ExecutedTimeAvg)
			if !topology.Benchmark {
				topology.Bolts[i].ExecutedTimeBenchmarkAvgSamples = append(topology.Bolts[i].ExecutedTimeBenchmarkAvgSamples, topology.Bolts[i].ExecutedTimeAvg)
			}
		}
	}
}

func updateInputBolt(bolt *storm.Bolt, api storm.MetricsAPI) {
	var inputRate int64
	for _, boltApi := range api.Bolts {
		for _, emitted := range boltApi.Emitted {
			if emitted.StreamID == bolt.Name {
				inputRate += int64(emitted.Value)
			}
		}
	}
	if inputRate > 0 {
		bolt.Input = inputRate - bolt.EmittedTotal
		bolt.EmittedTotal = inputRate
	}
}

func saveMetrics(topology storm.Topology) {
	for _, bolt := range topology.Bolts {
		if err := util.WriteCsv(topology.Id, bolt.Name, []storm.Bolt{bolt}); err != nil {
			log.Printf("error write csv: %v\n", err)
		}
	}
}
