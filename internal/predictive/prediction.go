package predictive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/spf13/viper"
	"log"
)

func PredictionInput(topology *storm.Topology) []float64 {
	var index int
	if index = len(topology.InputRate) - viper.GetInt("storm.adaptive.prediction_samples"); index < 0 {
		index = 0
	}

	var samples []float64
	for i := index; i < len(topology.InputRate); i++ {
		log.Printf("analyze: train: index={%d},sample={%v},\n", i, topology.InputRate[i])
	}

	return GetPrediction(samples, viper.GetInt("storm.adaptive.prediction_number"))
}
