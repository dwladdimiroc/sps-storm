package adaptive

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/montanaflynn/stats"
	"github.com/spf13/viper"
	"log"
)

func analyze(topology *storm.Topology) map[string]int {
	var stateBolts map[string]int
	log.Printf("analyze: period %v\n", period)
	if period%viper.GetInt("storm.adaptive.logical.predictive.number_samples") == 0 {
		//
	} else if period%viper.GetInt("storm.adaptive.logical.reactive.number_samples") == 0 {
		stateBolts = analyzeReactiveLogical(topology)
	}

	return stateBolts
}

func analyzeReactiveLogical(topology *storm.Topology) map[string]int {
	var stateBolts = make(map[string]int)
	for _, bolt := range topology.Bolts {
		if len(bolt.HistoryMetrics) >= viper.GetInt("storm.adaptive.logical.reactive.number_samples") {
			stateBolts[bolt.Name] = analyzeHistoryBolt(bolt.Name, bolt.HistoryMetrics)
		}
	}
	return stateBolts
}

func getHistoryBolt(metrics []float64, numberSamples int) []float64 {
	lastSamples := len(metrics) - 1
	var data = make([]float64, numberSamples)

	for i := 0; i < numberSamples; i++ {
		index := lastSamples - i
		data[i] = metrics[index]
	}

	var dataNorm []float64
	if stdDev, err := stats.StandardDeviation(data); err != nil {
		fmt.Printf("error get history bolt: %v\n", err)
	} else {
		if mean, err := stats.Mean(data); err != nil {
			fmt.Printf("error get history bolt: %v\n", err)
		} else {
			limitUpper := mean + stdDev
			limitLower := mean - stdDev
			for i := range data {
				if limitUpper >= data[i] && data[i] >= limitLower {
					dataNorm = append(dataNorm, data[i])
				}
			}
		}
	}

	return dataNorm
}

func analyzeHistoryBolt(name string, metrics []float64) int {
	historyBolt := getHistoryBolt(metrics, viper.GetInt("storm.adaptive.logical.reactive.number_samples"))
	if metric, err := stats.Mean(historyBolt); err != nil {
		fmt.Printf("error analyze history bolt %s: {mertrics: %v, error: %v}\n", name, metrics, err)
		return -2
	} else {
		var analyzeReactive = analyzeState(metric)
		return analyzeReactive
	}
}

func analyzePredictiveLogical() {

}

func analyzeState(metric float64) int {
	if metric >= viper.GetFloat64("storm.adaptive.logical.reactive.upper_limit") {
		return 1
	} else if metric <= viper.GetFloat64("storm.adaptive.logical.reactive.lower_limit") {
		return -1
	} else {
		return 0
	}
}
