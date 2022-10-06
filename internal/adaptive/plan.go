package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/spf13/viper"
	"log"
)

func planning(topology *storm.Topology) {
	for i := range topology.Bolts {
		if topology.Bolts[i].PredictionReplicas < 1 {
			topology.Bolts[i].Replicas = 1
		} else {
			if topology.Bolts[i].PredictionReplicas > viper.GetInt64("storm.adaptive.logical.reactive.limit_replicas") {
				topology.Bolts[i].Replicas = viper.GetInt64("storm.adaptive.logical.reactive.limit_replicas")
			} else {
				topology.Bolts[i].Replicas = topology.Bolts[i].PredictionReplicas
			}
		}
		log.Printf("Bolt={%s},InputRate={%d},ExecutedTime={%.2f},TimeWindows={%v},Replicas={%d}\n", topology.Bolts[i].Name, topology.InputRate, topology.Bolts[i].ExecutedTimeAvg, viper.GetInt("storm.adaptive.time_window_size"), topology.Bolts[i].PredictionReplicas)
	}
}
