package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
	"log"
	"strconv"
)

func monitor(topology *storm.Topology) bool {
	if ok, topologyMetrics := storm.GetMetrics(*topology); ok {
		log.Printf("[t=%d] monitor: update stats topology\n", period*viper.GetInt("storm.adaptive.time_window_size"))
		updateTopology(topology, topologyMetrics)
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

func updateTopology(topology *storm.Topology, metrics storm.TopologyMetrics) {
	updateStatsInputStream(topology, metrics)
	updateCompleteLatency(topology, metrics)
	updateStatsBolt(topology, metrics)
	updatePredictedInput(topology)
}

func updateStatsInputStream(topology *storm.Topology, metrics storm.TopologyMetrics) {
	var inputRate int64
	for _, spout := range metrics.Spouts {
		for _, outputStat := range spout.OutputStats {
			for i := range topology.Bolts {
				if outputStat.Stream == topology.Bolts[i].Name {
					topology.Bolts[i].Input += int64(outputStat.Emitted)
				}

			}
		}
		for _, stats := range spout.SpoutSummary {
			if stats.Window == ":all-time" {
				inputRate += int64(stats.Emitted)
			}
		}
	}

	for i := range topology.Bolts {
		inputBoltCurrent := topology.Bolts[i].Input - topology.Bolts[i].InputTotal
		topology.Bolts[i].InputTotal = topology.Bolts[i].Input
		topology.Bolts[i].Input = inputBoltCurrent
	}

	inputRateCurrent := inputRate - topology.InputRateAccum // difference between inputRate_{t} and inputRate_{t-1}
	topology.InputRateAccum = inputRate
	topology.InputRate = append(topology.InputRate, inputRateCurrent)
	//log.Printf("[monitor] period={%d},inputRate={%d}", period, inputRate)
}

// TODO This latency is not sure, it must be fixed
func updateCompleteLatency(topology *storm.Topology, metrics storm.TopologyMetrics) {
	var completeLatency float64
	for _, spout := range metrics.Spouts {
		for _, stats := range spout.SpoutSummary {
			if stats.Window == ":all-time" {
				completeLatency += stats.CompleteLatency * float64(stats.Emitted) / float64(topology.InputRate[len(topology.InputRate)-1])
			}
		}
	}
	for i := range topology.Bolts {
		topology.Bolts[i].CompleteLatency = completeLatency
	}
}

func updateStatsBolt(topology *storm.Topology, metrics storm.TopologyMetrics) {
	for _, bolt := range metrics.Bolts {
		updateOutputBolt(topology, bolt)
		updateExecutedAvg(topology, bolt)
	}

	for i := range topology.Bolts {
		topology.Bolts[i].Time = int64(period) * viper.GetInt64("storm.adaptive.time_window_size")
		updateInputBolt(&topology.Bolts[i], metrics)
	}

	for i := range topology.Bolts {
		updateQueue(&topology.Bolts[i])
	}
}

func updateOutputBolt(topology *storm.Topology, boltMetrics storm.BoltMetrics) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == boltMetrics.Id {
			for _, boltStats := range boltMetrics.BoltStats {
				if boltStats.Window == ":all-time" {
					topology.Bolts[i].Output = boltStats.Executed
				}
			}
			outputBoltCurrent := topology.Bolts[i].ExecutedTotal - topology.Bolts[i].Output
			topology.Bolts[i].ExecutedTotal = topology.Bolts[i].Output
			topology.Bolts[i].Output = outputBoltCurrent
		}
	}
}

func updateExecutedAvg(topology *storm.Topology, boltMetrics storm.BoltMetrics) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == boltMetrics.Id {
			for _, boltStats := range boltMetrics.BoltStats {
				if boltStats.Window == ":all-time" {
					executeLatency, _ := strconv.ParseFloat(boltStats.ExecuteLatency, 64)
					topology.Bolts[i].ExecutedTimeAvg = executeLatency
				}
			}

			topology.Bolts[i].ExecutedTimeAvgSamples = append(topology.Bolts[i].ExecutedTimeAvgSamples, topology.Bolts[i].ExecutedTimeAvg)
			if !topology.Benchmark {
				topology.Bolts[i].ExecutedTimeBenchmarkAvgSamples = append(topology.Bolts[i].ExecutedTimeBenchmarkAvgSamples, topology.Bolts[i].ExecutedTimeAvg)
			}
		}
	}
}

func updateInputBolt(bolt *storm.Bolt, topologyMetrics storm.TopologyMetrics) {
	for _, boltMetrics := range topologyMetrics.Bolts {
		for _, boltStats := range boltMetrics.OutputStats {
			if boltStats.Stream == bolt.Name {
				bolt.Input += boltStats.Emitted
			}
		}
	}

	inputBoltCurrent := bolt.Input - bolt.InputTotal
	bolt.InputTotal = bolt.Input
	bolt.Input = inputBoltCurrent
}

func updateQueue(bolt *storm.Bolt) {
	if bolt.Queue += bolt.Input - bolt.Output; bolt.Queue < 0 {
		bolt.Queue = 0
	}
}

func updatePredictedInput(topology *storm.Topology) {
	if len(topology.PredictedInputRate) > 0 {
		for i := range topology.Bolts {
			topology.Bolts[i].PredictedInput = topology.PredictedInputRate[period]
		}
	}
}

func saveMetrics(topology storm.Topology) {
	for _, bolt := range topology.Bolts {
		if err := util.WriteCsv(topology.Id, bolt.Name, []storm.Bolt{bolt}); err != nil {
			log.Printf("error write csv: %v\n", err)
		}
	}
}
