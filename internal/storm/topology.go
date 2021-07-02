package storm

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/montanaflynn/stats"
	"github.com/spf13/viper"
	"math"
	"reflect"
)

type Bolt struct {
	Name                     string    `csv:"name"`
	Time                     int64     `csv:"time"`
	Replicas                 int64     `csv:"replicas"`
	PredictionReplicas       int64     `csv:"prediction_replicas"`
	Input                    int64     `csv:"input"`
	Output                   int64     `csv:"output"`
	ExecutedTimeAvg          float64   `csv:"executed_time_avg"`
	ExecutedTimeBenchmarkAvg float64   `csv:"executed_time_benchmark_avg"`
	ExecutedTimeAvgSamples   []float64 `csv:"-"`
	LatencyMetric            float64   `csv:"latency_metric"`
	Utilization              float64   `csv:"utilization"`
	Queue                    int64     `csv:"queue"`
	QueueMetric              float64   `csv:"queue_metric"`
	//EventLoss                int64              `csv:"event_loss"`
	//EventLossAccum           int64              `csv:"event_loss_ash ccum"`
	ExecutedTotal   int64              `csv:"executed_total"`
	Metric          float64            `csv:"metric"`
	CompleteLatency float64            `csv:"complete_latency"`
	Location        map[string]float64 `csv:"-"`
	HistoryMetrics  []float64          `csv:"-"`
	AlertMetrics    []int              `csv:"-"`
}

func (b *Bolt) CalculateStats() {
	b.calculateLatencyMetric()
	b.calculateQueue()
	b.calculateUtilization()
	b.calculateQueueMetric()
	b.calculatePredictionReplicas()
	b.calculateMetric()
}

func (b *Bolt) calculateLatencyMetric() {
	if b.ExecutedTimeBenchmarkAvg == 0 {
		b.LatencyMetric = 0
	} else {
		if !math.IsNaN(b.ExecutedTimeAvg) {
			b.LatencyMetric = 1 - (b.ExecutedTimeBenchmarkAvg / b.ExecutedTimeAvg)
			if b.LatencyMetric < 0 {
				b.LatencyMetric = 0
			}
		} else {
			b.LatencyMetric = 0
		}
	}
}

func (b *Bolt) calculateQueue() {
	b.Queue += b.Input - b.Output
	b.Queue -= int64(math.Floor(0.05 * float64(b.Output)))
	if b.Queue < 0 {
		b.Queue = 0
	}
}

func (b *Bolt) calculateUtilization() {
	if math.IsNaN(b.ExecutedTimeAvg) {
		b.Utilization = 0
	} else {
		var executedAvg float64
		if b.ExecutedTimeAvg < b.ExecutedTimeBenchmarkAvg {
			executedAvg = b.ExecutedTimeBenchmarkAvg
		} else {
			executedAvg = b.ExecutedTimeAvg
		}
		b.Utilization = (executedAvg * float64(b.Output)) / (float64(b.Replicas * int64(viper.GetInt("storm.adaptive.time_window_size")) * util.SECS))
	}
}

func (b *Bolt) calculateQueueMetric() {
	if b.Queue == 0 {
		b.QueueMetric = 0
	} else {
		if x := float64(b.Output) / float64(b.Queue); x < 0 {
			x *= -1
		} else {
			b.QueueMetric = 1 - x
			if b.QueueMetric < 0 {
				b.QueueMetric = 0
			}
		}
	}
}

func (b *Bolt) calculatePredictionReplicas() {
	if b.Queue > 0 {
		x := float64(b.Output) / float64(b.Input)
		if x < 1 {
			b.PredictionReplicas = int64(math.Ceil(1 / x))
		}
	}
}

func (b *Bolt) calculateMetric() {
	b.Time += 5
	b.Metric = viper.GetFloat64("storm.adaptive.logical.metric.throughput_weight")*b.Utilization +
		viper.GetFloat64("storm.adaptive.logical.metric.latency_weight")*b.LatencyMetric +
		viper.GetFloat64("storm.adaptive.logical.metric.queue_weight")*b.QueueMetric
	b.HistoryMetrics = append(b.HistoryMetrics, b.Metric)
}

func (b *Bolt) clearStatsTimeWindow() {
	b.Input = 0
	b.Output = 0
	b.ExecutedTimeAvg = 0
	//b.LatencyMetric = 0
	b.Utilization = 0
	b.QueueMetric = 0
	b.Metric = 0
}

type Topology struct {
	Id        string
	Benchmark bool
	Bolts     []Bolt
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

	if err := util.CreateDir(t.Id); err != nil {
		fmt.Printf("error mkdir: %v\n", err)
	}

	for _, bolt := range t.Bolts {
		if err := util.CreateCsv(t.Id, bolt.Name, []Bolt{}); err != nil {
			fmt.Printf("error create csv: %v\n", err)
		}
	}
}

func (t *Topology) ClearStatsTimeWindow() {
	for i := range t.Bolts {
		t.Bolts[i].clearStatsTimeWindow()
	}
}

func (t *Topology) BenchmarkExecutedTimeAvg() {
	t.Benchmark = true

	for i := range t.Bolts {
		var samples []float64
		for j := range t.Bolts[i].ExecutedTimeAvgSamples {
			if !math.IsNaN(t.Bolts[i].ExecutedTimeAvgSamples[j]) {
				samples = append(samples, t.Bolts[i].ExecutedTimeAvgSamples[j])
			}
		}

		var normSamples []float64
		meanSamples, _ := stats.Mean(samples)
		stdDevSamples, _ := stats.StandardDeviation(samples)
		upperLimit := meanSamples + stdDevSamples
		lowerLimit := meanSamples - stdDevSamples
		for j := range samples {
			if lowerLimit <= samples[j] && samples[j] <= upperLimit {
				normSamples = append(normSamples, samples[j])
			}
		}

		t.Bolts[i].ExecutedTimeBenchmarkAvg, _ = stats.Mean(normSamples)
	}
}
