package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
	"log"
	"math"
)

func planning(stateBolts map[string]int, topology *storm.Topology) {
	log.Printf("planning: %v\n", stateBolts)
	for nameBolt, stateBolt := range stateBolts {
		if stateBolt > 0 {
			addReplicaBolt(nameBolt, topology)
		} else if stateBolt < 0 {
			removeReplicaBolt(nameBolt, topology)
		}
	}
}

func addReplicaBolt(nameBolt string, topology *storm.Topology) {
	for i := range topology.Bolts {
		metric := (viper.GetFloat64("storm.adaptive.logical.reactive.upper_limit") + viper.GetFloat64("storm.adaptive.logical.reactive.lower_limit")) / float64(2)
		timeWindow := int64(viper.GetInt("storm.adaptive.time_window_size")) * util.SECS
		replicasPredictive := math.Ceil((float64(topology.InputRate) * topology.Bolts[i].ExecutedTimeBenchmarkAvg) / (metric * float64(timeWindow)))
		//fmt.Printf("metric {%v} inputRate {%v} ExecutedTimeAvg{%v} timeWindows{%v}", metric, topology.InputRate, topology.Bolts[i].ExecutedTimeAvg, timeWindow)
		if replicasPredictive < 1 {
			replicasPredictive = 1
		}
		topology.Bolts[i].Replicas = int64(replicasPredictive)
		//fmt.Printf("Bolt {%s} Replica {%d}\n", topology.Bolts[i].Name, topology.Bolts[i].Replicas)
	}
}

func removeReplicaBolt(nameBolt string, topology *storm.Topology) {
	for i := range topology.Bolts {
		metric := (viper.GetFloat64("storm.adaptive.logical.reactive.upper_limit") + viper.GetFloat64("storm.adaptive.logical.reactive.lower_limit")) / float64(2)
		timeWindow := int64(viper.GetInt("storm.adaptive.time_window_size")) * util.SECS
		replicasPredictive := math.Ceil((float64(topology.InputRate) * topology.Bolts[i].ExecutedTimeBenchmarkAvg) / (metric * float64(timeWindow)))
		if replicasPredictive < 1 {
			replicasPredictive = 1
		}
		topology.Bolts[i].Replicas = int64(replicasPredictive)
		//fmt.Printf("Bolt {%s} Replica {%d}\n", topology.Bolts[i].Name, topology.Bolts[i].Replicas)
	}
}
