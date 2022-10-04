package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
)

func planning(topology *storm.Topology) {
	modifyReplicaBolt(topology)
	topology.InputRate = 0
}

//func predictionInput(history []float64) float64 {
//	tw := viper.GetInt("storm.adaptive.time_window_size") * util.SECS
//
//	r := new(regression.Regression)
//	r.SetObserved("input")
//	r.SetVar(0, "time")
//	for i := 0; i < len(history); i++ {
//
//	}
//
//		prediction, _ := r.Predict([]float64{tw*i+5})
//		return prediction}
//
//}

func modifyReplicaBolt(topology *storm.Topology) {
	//for i := range topology.Bolts {
	//	input := predictionInput(topology.HistoryInputRate)
	//	metric := (viper.GetFloat64("storm.adaptive.logical.reactive.upper_limit") + viper.GetFloat64("storm.adaptive.logical.reactive.lower_limit")) / 2
	//	var replicasPredictive float64
	//	if topology.Bolts[i].ExecutedTimeBenchmarkAvg > topology.Bolts[i].ExecutedTimeAvg {
	//		replicasPredictive = (input * topology.Bolts[i].ExecutedTimeBenchmarkAvg) / (float64(int64(viper.GetInt("storm.adaptive.time_window_size"))*util.SECS) * metric)
	//	} else {
	//		replicasPredictive = (input * topology.Bolts[i].ExecutedTimeAvg) / (float64(int64(viper.GetInt("storm.adaptive.time_window_size"))*util.SECS) * metric)
	//	}
	//	if replicasPredictive < 1 {
	//		topology.Bolts[i].Replicas = 1
	//	} else {
	//		replicas := int64(math.Ceil(replicasPredictive))
	//		if replicas > viper.GetInt64("storm.adaptive.logical.reactive.limit_replicas") {
	//			topology.Bolts[i].Replicas = viper.GetInt64("storm.adaptive.logical.reactive.limit_replicas")
	//		} else {
	//			topology.Bolts[i].Replicas = replicas
	//		}
	//	}
	//	log.Printf("Bolt={%s},InputRate={%d},InputRateF={%.2f},ExecutedTime={%.2f},TimeWindows={%v},Metric={%.2f},Replicas={%.2f}\n", topology.Bolts[i].Name, topology.InputRate, input, topology.Bolts[i].ExecutedTimeAvg, viper.GetInt("storm.adaptive.time_window_size"), metric, replicasPredictive)
	//}
}
