package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/montanaflynn/stats"
	"github.com/sajari/regression"
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
		log.Printf("[Analyze] Bolt={%s},Prediction={%d}", topology.Bolts[i].Name, topology.Bolts[i].PredictionReplicas)
	}
}

func predictionInput(topology *storm.Topology) int64 {
	var inputRegression = new(regression.Regression)
	inputRegression.SetObserved("input")
	inputRegression.SetVar(0, "time")

	for i := range topology.InputRate {
		inputRegression.Train(regression.DataPoint(float64(topology.InputRate[i]), []float64{float64(i)}))
	}

	if err := inputRegression.Run(); err != nil {
		log.Printf("error predictive input: %v\n", err)
	}
	log.Printf("[predictionInput] %s\n", inputRegression.String())

	var predInput []float64
	for i := 1; i <= viper.GetInt("storm.adaptive.prediction_samples"); i++ {
		if sample, err := inputRegression.Predict([]float64{float64(period + i)}); err != nil {
			log.Printf("error predictive input: %v\n", err)
		} else {
			log.Printf("[predictionInput] period={%d},i={%d},sample={%v},\n", period, i, sample)
			predInput = append(predInput, sample)
		}
	}

	log.Printf("[predictionInput] predInput={%v}\n", predInput)
	if input, err := stats.Mean(predInput); err != nil {
		log.Printf("error mean input: %v\n", err)
		return 0
	} else {
		return int64(math.Ceil(input))
	}
}

func predictionReplicas(input int64, bolt storm.Bolt) int64 {
	executedTimeAvg := chooseExecutedTime(bolt)
	replicasPredictive := (float64(input) * executedTimeAvg) / (float64(int64(viper.GetInt("storm.adaptive.time_window_size")) * util.SECS))
	return int64(replicasPredictive)
}

func chooseExecutedTime(bolt storm.Bolt) float64 {
	executedTimeAvg := bolt.GetExecutedTimeAvg()
	if bolt.ExecutedTimeBenchmarkAvg > executedTimeAvg {
		return bolt.ExecutedTimeBenchmarkAvg
	} else {
		return executedTimeAvg
	}
}
