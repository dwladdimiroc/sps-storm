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
	input := getInput(topology)
	//log.Printf("input predicted: %d\n", input)
	for i := range topology.Bolts {
		topology.Bolts[i].PredictionReplicas = predictionReplicas(input, topology.Bolts[i])
		//log.Printf("analyze: bolt={%s},prediction={%d}", topology.Bolts[i].Name, topology.Bolts[i].PredictionReplicas)
	}
}

func getInput(topology *storm.Topology) int64 {
	var input int64
	var samplesF64 []float64
	var index int
	if index = len(topology.InputRate) - viper.GetInt("storm.adaptive.input_samples"); index < 0 {
		index = 0
	}
	for i := index; i < len(topology.InputRate); i++ {
		samplesF64 = append(samplesF64, float64(topology.InputRate[i]))
	}
	if viper.GetString("storm.adaptive.prediction_input") == "lineal" {
		log.Printf("analyse: prediction_input: lineal regression\n")
		input = predictionLinealInput(topology)
	} else if viper.GetString("storm.adaptive.prediction_input") == "fft" {
		log.Printf("analyse: prediction_input: fft\n")
		predictionFFTInput()
	} else { // basic
		log.Printf("analyse: prediction_input: basic\n")
		input = topology.InputRate[len(topology.InputRate)-1]
	}
	return input
}

func predictionLinealInput(topology *storm.Topology) int64 {
	var inputRegression = new(regression.Regression)
	inputRegression.SetObserved("input")
	inputRegression.SetVar(0, "time")

	//log.Printf("analyze: input={%v}\n", topology.InputRate)
	var index int
	if index = len(topology.InputRate) - viper.GetInt("storm.adaptive.input_samples"); index < 0 {
		index = 0
	}
	for i := index; i < len(topology.InputRate); i++ {
		log.Printf("analyze: train: index={%d},sample={%v},\n", i, topology.InputRate[i])
		inputRegression.Train(regression.DataPoint(float64(topology.InputRate[i]), []float64{float64(i)}))
	}

	if err := inputRegression.Run(); err != nil {
		log.Printf("error predict input: %v\n", err)
	}
	//log.Printf("[predictionLinealInput] %s\n", inputRegression.String())

	var predInput []float64
	var indexPrediction = index + viper.GetInt("storm.adaptive.input_samples")
	if len(topology.InputRate) < viper.GetInt("storm.adaptive.input_samples") {
		indexPrediction = index + len(topology.InputRate)
	}
	for i := indexPrediction; i < indexPrediction+viper.GetInt("storm.adaptive.input_predict"); i++ {
		if sample, err := inputRegression.Predict([]float64{float64(i)}); err != nil {
			log.Printf("error predict input: %v\n", err)
		} else {
			log.Printf("analyze: predict: index={%d},sample={%v},\n", i, sample)
			predInput = append(predInput, sample)
		}
	}

	//log.Printf("[predictionLinealInput] predInput={%v}\n", predInput)
	if input, err := stats.Mean(predInput); err != nil {
		log.Printf("error mean input: %v\n", err)
		return 0
	} else {
		//log.Printf("analyze: prediction input={%v}\n", int64(math.Ceil(input)))
		return int64(math.Ceil(input))
	}
}

func predictionFFTInput() {

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
