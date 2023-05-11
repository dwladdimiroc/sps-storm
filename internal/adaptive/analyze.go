package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/predictive"
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/montanaflynn/stats"
	"github.com/spf13/viper"
	"log"
	"math"
)

func analyze(topology *storm.Topology) {
	log.Printf("analyze: period %v\n", period)
	predictedInput := getInput(topology)
	//log.Printf("input predicted: %d\n", input)
	for i := range topology.Bolts {
		predictedInput += predictionInputQueue(topology.Bolts[i], *topology) / viper.GetInt64("storm.adaptive.prediction_number")
		topology.Bolts[i].PredictionReplicas = predictionReplicas(predictedInput, topology.Bolts[i])
		log.Printf("analyze: bolt={%s},predictionInput={%d},predictionReplicas={%d}", topology.Bolts[i].Name, predictedInput, topology.Bolts[i].PredictionReplicas)
	}
}

func getInput(topology *storm.Topology) int64 {
	var samplesPrediction []float64

	if viper.GetString("storm.adaptive.predictive_model") == "basic" {
		//log.Printf("analyse: prediction_input: basic\n")
		samplesPrediction = predictive.Simple(topology)
	} else {
		//log.Printf("analyse: prediction_input: %s\n", viper.GetString("storm.adaptive.predictive_model"))
		samplesPrediction = predictive.PredictionInput(topology)
	}

	if input, err := stats.Mean(samplesPrediction); err != nil {
		log.Printf("error mean input: %v\n", err)
		return 0
	} else {
		//log.Printf("analyze: prediction_input: {%s}, samples ={%v}, prediction input={%v}\n", viper.GetString("storm.adaptive.predictive_model"), samplesPrediction, int64(math.Ceil(input)))
		return int64(math.Ceil(input))
	}
}

func predictionInputQueue(bolt storm.Bolt, topology storm.Topology) int64 {
	var valuePredictionQ = bolt.Queue
	for _, tagBoltPredecessor := range bolt.BoltsPredecessor {
		for _, boltTopology := range topology.Bolts {
			if boltTopology.Name == tagBoltPredecessor {
				valuePredictionQ += predictionInputQueue(boltTopology, topology)
			}
		}
	}

	return valuePredictionQ
}

func predictionReplicas(input int64, bolt storm.Bolt) int64 {
	executedTimeAvg := chooseExecutedTime(bolt)
	timeWindow := float64(int64(viper.GetInt("storm.adaptive.time_window_size")) * util.SECS)
	replicasPredictive := float64(input) * executedTimeAvg / timeWindow
	//log.Printf("analyze: prediction replicas={%v},input={%v},execTime={%v},timeWindow={%v}\n", replicasPredictive, input, executedTimeAvg, timeWindow)
	return int64(math.Ceil(replicasPredictive))
}

func chooseExecutedTime(bolt storm.Bolt) float64 {
	executedTimeAvg := bolt.GetExecutedTimeAvg()
	if bolt.ExecutedTimeBenchmarkAvg > executedTimeAvg {
		return bolt.ExecutedTimeBenchmarkAvg
	} else {
		return executedTimeAvg
	}
}
