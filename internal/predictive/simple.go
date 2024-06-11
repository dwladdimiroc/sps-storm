package predictive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/spf13/viper"
)

func Simple(topology *storm.Topology) []float64 {
	var predictions []float64
	for i := len(topology.InputRate) - viper.GetInt("storm.adaptive.prediction_number"); i < len(topology.InputRate); i++ {
		predictions = append(predictions, float64(topology.InputRate[i]))
	}
	return predictions
}
