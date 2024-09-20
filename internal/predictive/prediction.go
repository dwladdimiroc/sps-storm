package predictive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/spf13/viper"
	"log"
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

func GetAllPred() []PredictionInput {
	return predictions
}

func GetPred() PredictionInput {
	return predictions[indexChosenPredictor]
}

func InitPrediction() {
	var basic, lr, fft, ann, rf, svm, bayesian, ridge, gaussian, sgd PredictionInput
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
	//SVM
	svm.NameModel = "svm"
	svm.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, svm)
	//Bayesian
	bayesian.NameModel = "bayesian"
	bayesian.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, bayesian)
	//Ridge
	ridge.NameModel = "ridge"
	ridge.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, ridge)
	//Gaussian
	gaussian.NameModel = "gaussian"
	gaussian.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, gaussian)
	//MPL (ANN)
	sgd.NameModel = "sgd"
	sgd.PredictedInput = make([]float64, viper.GetInt("storm.adaptive.analyze_samples"))
	predictions = append(predictions, sgd)
}

func PredictInput(topology *storm.Topology, period int) {
	predictions[0].PredictedInput = append(predictions[0].PredictedInput, Simple(topology)...)

	var wg sync.WaitGroup

	for i := 1; i < len(predictions); i++ {
		wg.Add(1)
		go GetPredictionInput(topology, i, &wg, period)
	}

	wg.Wait()
}

func GetPredictionInput(topology *storm.Topology, indexPredictor int, wg *sync.WaitGroup, period int) {
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

	resultsPrediction := GetPrediction(samples, viper.GetInt("storm.adaptive.prediction_number"), predictions[indexPredictor].NameModel)
	log.Printf("[t=%d] analyze: get prediction={%s}: samples={%v},lenSamples={%d},prediction={%v},lenPrediction={%d}\n", period, predictions[indexPredictor].NameModel, samples, len(samples), resultsPrediction, len(resultsPrediction))
	predictions[indexPredictor].PredictedInput = append(predictions[indexPredictor].PredictedInput, resultsPrediction...)
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

	if viper.GetString("storm.adaptive.predictive_model") == "multi" {
		indexChosenPredictor = indexPredictor
	} else {
		for i := 0; i < len(predictions); i++ {
			if predictions[i].NameModel == viper.GetString("storm.adaptive.predictive_model") {
				indexChosenPredictor = i
				return
			}
		}
	}
}

// RMSE
func calculateError(input []int64) {
	for k := 0; k < len(predictions); k++ {
		var errorEst float64
		for i := len(input) - viper.GetInt("storm.adaptive.prediction_number"); i < len(input); i++ {
			errorEst += math.Pow(predictions[k].PredictedInput[i]-float64(input[i]), 2.0)
		}
		predictions[k].ErrorEstimation = math.Sqrt(errorEst / float64(viper.GetInt("storm.adaptive.prediction_number")))
	}
}

func GetPredictedInputPeriod(period int) int64 {
	predictedInputPeriod := int64(predictions[indexChosenPredictor].PredictedInput[period])
	//log.Printf("predicted input period : %d perdiction={%v}", period, predictions[indexChosenPredictor])
	return predictedInputPeriod
}
