package storm

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
	"math"
	"reflect"
)

type Bolt struct {
	Time               int64   `csv:"time"`
	Name               string  `csv:"name"`
	Replicas           int64   `csv:"replicas"`
	PredictionReplicas int64   `csv:"prediction_replicas"`
	Input              int64   `csv:"input"`
	Output             int64   `csv:"output"`
	ExecutedTimeAvg    float64 `csv:"executed_time_avg"`
	//TransmitterTimeAvg float64 `csv:"transmitter_time_avg"`
	LatencyAvg    float64            `csv:"latency_avg"`
	Utilization   float64            `csv:"utilization"`
	Queue         int64              `csv:"queue"`
	QueueMetric   float64            `csv:"queue_metric"`
	ExecutedTotal int64              `csv:"executed_total"`
	Metric        float64            `csv:"metric"`
	Location      map[string]float64 `csv:"-"`
}

func (b *Bolt) CalculateUtilization() {
	b.Utilization = (b.ExecutedTimeAvg * float64(b.Output)) / (float64(viper.GetInt("storm.adaptive.time_window")) * float64(1000))
}

func (b *Bolt) CalculateQueueMetric() {
	b.QueueMetric = 1 - (float64(b.Output*b.Replicas) / float64(b.Input))
	if b.QueueMetric < 0 {
		b.QueueMetric = 0
	}
}

func (b *Bolt) CalculatePredictionReplicas() {
	b.Queue += b.Input - b.Output
	if b.Queue > 0 {
		x := float64(b.Output) / float64(b.Queue)
		if x < 1 {
			b.PredictionReplicas = int64(math.Ceil(1 / x))
		}
	}
}

func (b *Bolt) CalculateMetric() {
	b.Time += 5
	b.Metric = viper.GetFloat64("storm.adaptive.reactive.throughput_weight")*b.Utilization +
		viper.GetFloat64("storm.adaptive.reactive.latency_weight")*b.LatencyAvg +
		viper.GetFloat64("storm.adaptive.reactive.queue_weight")*b.QueueMetric
}

func (b *Bolt) ClearStatsTimeWindow() {
	b.Input = 0
	b.Output = 0
	b.ExecutedTimeAvg = 0
	b.LatencyAvg = 0
	b.Utilization = 0
	b.QueueMetric = 0
	b.Metric = 0
}

type Topology struct {
	Id    string
	Bolts []Bolt
}

func (t *Topology) CreateTopology(summaryTopology SummaryTopology) {
	for _, boltCurrent := range summaryTopology.Bolts {
		var bolt = Bolt{
			Name:     boltCurrent.BoltID,
			Replicas: 1,
			Location: make(map[string]float64),
		}
		t.Bolts = append(t.Bolts, bolt)
	}

	for _, worker := range summaryTopology.Workers {
		var machine = worker.Host + " " + worker.SupervisorID

		v := reflect.ValueOf(worker.ComponentNumTasks)
		if v.Kind() == reflect.Map {
			for _, key := range v.MapKeys() {
				valueMap := v.MapIndex(key)
				for i := range t.Bolts {
					if t.Bolts[i].Name == key.String() {
						t.Bolts[i].Location[machine] = valueMap.Interface().(float64)
					}
				}
			}
		}
	}

	for _, bolt := range t.Bolts {
		if err := util.CreateCsv(bolt.Name, []Bolt{}); err != nil {
			fmt.Printf("error create csv: %v\n", err)
		}
	}
}
