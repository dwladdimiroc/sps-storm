package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/montanaflynn/stats"
	"github.com/spf13/viper"
	"log"
	"math"
)

func analyze(topology *storm.Topology) {
	log.Printf("analyze: period %v\n", period)
	input := predictionInput(topology)
	log.Printf("input predicted: %d\n", input)
	for i := range topology.Bolts {
		topology.Bolts[i].PredictionReplicas = predictionReplicas(input, topology.Bolts[i])
		log.Printf("bolt %d prediction %d", i, topology.Bolts[i].PredictionReplicas)
	}
}

func predictionInput(topology *storm.Topology) int64 {
	if err := topology.InputRegression.Run(); err != nil {
		log.Printf("error predictive input: %v\n", err)
	}

	var predInput []float64
	for i := 1; i <= viper.GetInt("storm.adaptive.prediction_samples"); i++ {
		if sample, err := topology.InputRegression.Predict([]float64{float64(period + i)}); err != nil {
			log.Printf("error predictive input: %v\n", err)
		} else {
			predInput = append(predInput, sample)
		}
	}
	if input, err := stats.Mean(predInput); err != nil {
		log.Printf("error mean input: %v\n", err)
		return 0
	} else {
		return int64(math.Floor(input))
	}
}

func predictionReplicas(input int64, bolt storm.Bolt) int64 {
	executedTimeAvg := chooseExecutedTime(bolt)
	replicasPredictive := (float64(input) * executedTimeAvg) / (float64(int64(viper.GetInt("storm.adaptive.time_window_size")) * util.SECS))
	return int64(replicasPredictive)
}

func chooseExecutedTime(bolt storm.Bolt) float64 {
	if bolt.ExecutedTimeBenchmarkAvg > bolt.ExecutedTimeAvg {
		return bolt.ExecutedTimeBenchmarkAvg
	} else {
		return bolt.ExecutedTimeAvg
	}
}
