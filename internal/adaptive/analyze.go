package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/predictive"
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
	"log"
	"math"
)

func analyze(topology *storm.Topology) {
	log.Printf("analyze: period %v\n", period)
	if period%viper.GetInt("storm.adaptive.analyze_samples") == 0 {
		log.Printf("analyze: prediction %v\n", period)
		predictive.PredictInput(topology)
		for i := range topology.Bolts {
			topology.Bolts[i].PredictionQueue = predictionInputQueue(topology.Bolts[i], *topology) / viper.GetInt64("storm.adaptive.analyze_samples")
		}
		predictive.DeterminatePredictor(topology)
		topology.ClearQueue()
		topology.PredictedInputRate = make([]int64, len(predictive.GetPred().PredictedInput))
		for i := 0; i < len(predictive.GetPred().PredictedInput); i++ {
			topology.PredictedInputRate[i] = int64(predictive.GetPred().PredictedInput[i])
		}
		log.Printf("analyze: prediction: model={%s}", predictive.GetPred().NameModel)
	}

	//log.Printf("input predicted: %d\n", input)
	if period >= viper.GetInt("storm.adaptive.analyze_samples") {
		log.Printf("analyze: determinate replicas %v\n", period)
		for i := range topology.Bolts {
			predictedInput := topology.Bolts[i].PredictionQueue + predictive.GetPredictedInputPeriod(period)
			topology.Bolts[i].PredictionReplicas = predictionReplicas(predictedInput, topology.Bolts[i])
			log.Printf("analyze: bolt={%s},predictionInput={%d},predictionReplicas={%d}", topology.Bolts[i].Name, predictedInput, topology.Bolts[i].PredictionReplicas)
		}
		planning(topology)
		execute(*topology)
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
