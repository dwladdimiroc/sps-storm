package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/spf13/viper"
	"log"
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
		if topology.Bolts[i].Name == nameBolt {
			if viper.GetInt64("storm.adaptive.logical.reactive.limit_replicas") == 0 || viper.GetInt64("storm.adaptive.logical.reactive.limit_replicas") > topology.Bolts[i].Replicas {
				if viper.GetFloat64("storm.adaptive.logical.metric.latency_weight") == 0 {
					topology.Bolts[i].Replicas += viper.GetInt64("storm.adaptive.logical.reactive.number_replicas")
				} else {
					if topology.Bolts[i].LatencyMetric < viper.GetFloat64("storm.adaptive.logical.metric.latency_limit") {
						topology.Bolts[i].Replicas += viper.GetInt64("storm.adaptive.logical.reactive.number_replicas")
					}
				}
			}
		}
	}
}

func removeReplicaBolt(nameBolt string, topology *storm.Topology) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == nameBolt {
			if topology.Bolts[i].Replicas > 1 {
				topology.Bolts[i].Replicas -= viper.GetInt64("storm.adaptive.logical.reactive.number_replicas")
				if topology.Bolts[i].Replicas < 1 {
					topology.Bolts[i].Replicas = 1
				}
			}
		}
	}
}
