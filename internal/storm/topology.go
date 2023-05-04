package storm

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/montanaflynn/stats"
	"math"
	"reflect"
	"strings"
)

type Bolt struct {
	Name                            string             `csv:"name"`
	Time                            int64              `csv:"time"`
	Replicas                        int64              `csv:"replicas"`
	PredictionReplicas              int64              `csv:"prediction_replicas"`
	Input                           int64              `csv:"input"`
	Output                          int64              `csv:"output"`
	ExecutedTimeAvg                 float64            `csv:"executed_time_avg"`
	ExecutedTimeAvgSamples          []float64          `csv:"-"`
	ExecutedTimeBenchmarkAvg        float64            `csv:"executed_time_benchmark_avg"`
	ExecutedTimeBenchmarkAvgSamples []float64          `csv:"-"`
	EmittedTotal                    int64              `csv:"emitted_total"`
	ExecutedTotal                   int64              `csv:"executed_total"`
	CompleteLatency                 float64            `csv:"complete_latency"`
	VirtualMachines                 map[string]float64 `csv:"-"`
}

func (b *Bolt) clearStatsTimeWindow() {
	b.Input = 0
	b.Output = 0
	b.ExecutedTimeAvg = 0
}

func (b *Bolt) GetExecutedTimeAvg() float64 {
	v, _ := stats.Mean(b.ExecutedTimeAvgSamples)
	b.ExecutedTimeAvgSamples = nil
	return v
}

type Topology struct {
	Id         string
	Benchmark  bool
	InputAccum int64
	InputRate  []int64
	Bolts      []Bolt
}

func (t *Topology) Init(id string) {
	t.Id = id
}

func (t *Topology) CreateTopology(summaryTopology SummaryTopology) {
	for _, boltCurrent := range summaryTopology.Bolts {
		if !strings.Contains(boltCurrent.BoltID, "__") {
			var bolt = Bolt{
				Name:            boltCurrent.BoltID,
				Replicas:        1,
				VirtualMachines: make(map[string]float64),
			}
			t.Bolts = append(t.Bolts, bolt)
		}
	}

	for _, worker := range summaryTopology.Workers {
		var machine = worker.Host + " " + worker.SupervisorID

		v := reflect.ValueOf(worker.ComponentNumTasks)
		if v.Kind() == reflect.Map {
			for _, key := range v.MapKeys() {
				valueMap := v.MapIndex(key)
				for i := range t.Bolts {
					if t.Bolts[i].Name == key.String() {
						t.Bolts[i].VirtualMachines[machine] = valueMap.Interface().(float64)
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
		for j := range t.Bolts[i].ExecutedTimeBenchmarkAvgSamples {
			if !math.IsNaN(t.Bolts[i].ExecutedTimeBenchmarkAvgSamples[j]) {
				samples = append(samples, t.Bolts[i].ExecutedTimeBenchmarkAvgSamples[j])
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
