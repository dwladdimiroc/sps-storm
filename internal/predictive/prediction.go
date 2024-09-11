package predictive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/spf13/viper"
	"math"
	"sync"
)

var indexChosenPredictor int
var predictions []PredictionInput

type PredictionInput struct {
	NameModel       string
	PredictedInput  []float64
	ErrorEstimation float64
}

func GetPred() PredictionInput {
	return predictions[indexChosenPredictor]
}

func InitPrediction() {
	var basic, lr, fft, ann, rf PredictionInput
	//Basic
	basic.NameModel = "basic"
	basic.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, basic)
	//LR
	lr.NameModel = "linear_regression"
	lr.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, lr)
	//FFT
	fft.NameModel = "fft"
	fft.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, fft)
	//RF
	rf.NameModel = "random_forest"
	rf.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, rf)
	//MPL (ANN)
	ann.NameModel = "ann"
	ann.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, ann)
}

func PredictInput(topology *storm.Topology) {
	predictions[0].PredictedInput = append(predictions[0].PredictedInput, Simple(topology)...)

	var wg sync.WaitGroup

	for i := 1; i < len(predictions); i++ {
		wg.Add(1)
		go GetPredictionInput(topology, i, &wg)
	}

	wg.Wait()
}

func GetPredictionInput(topology *storm.Topology, indexPredictor int, wg *sync.WaitGroup) {
	defer wg.Done()

	var samples []float64

	var index int
	if index = len(topology.InputRate) - viper.GetInt("storm.adaptive.prediction_samples"); index < 0 {
		index = 0
	}
	for i := index; i < len(topology.InputRate); i++ {
		samples = append(samples, float64(topology.InputRate[i]))
		//log.Printf("analyze: train: index={%d},sample={%v},\n", i, topology.InputRate[i])
	}

	//log.Printf("analyze: get prediction: samples={%v}\n", samples)
	predictions[indexPredictor].PredictedInput = append(predictions[indexPredictor].PredictedInput, GetPrediction(samples, viper.GetInt("storm.adaptive.prediction_number"), predictions[indexPredictor].NameModel)...)
}

func DeterminatePredictor(topology *storm.Topology) {
	//Calculate error
	calculateError(topology.InputRate)

	//Determinate the best predictor
	indexPredictor := 0
	minErrorEstimation := predictions[indexPredictor].ErrorEstimation
	for i := 1; i < len(predictions); i++ {
		if predictions[i].ErrorEstimation < minErrorEstimation {
			indexPredictor = i
			minErrorEstimation = predictions[i].ErrorEstimation
		}
	}

	indexChosenPredictor = indexPredictor
}

func calculateError(input []int64) {
	for k := 0; k < len(predictions); k++ {
		var errorEst float64
		for i := len(input) - viper.GetInt("storm.adaptive.prediction_number"); i < len(input); i++ {
			errorEst += math.Abs(predictions[k].PredictedInput[i]-float64(input[i])) / float64(input[i])
		}
		predictions[k].ErrorEstimation = errorEst / float64(len(input))
	}
}

func GetPredictedInputPeriod(period int) int64 {
	predictedInputPeriod := int64(predictions[indexChosenPredictor].PredictedInput[period])
	//log.Printf("predicted input period : %d perdiction={%v}", period, predictions[indexChosenPredictor])
	return predictedInputPeriod
}
