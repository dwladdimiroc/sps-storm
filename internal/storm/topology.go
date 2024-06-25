package storm

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/montanaflynn/stats"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

type Bolt struct {
	Name                            string    `csv:"name"`
	Time                            int64     `csv:"time"`
	Replicas                        int64     `csv:"replicas"`
	PredictionReplicas              int64     `csv:"prediction_replicas"`
	Input                           int64     `csv:"input"`
	InputTotal                      int64     `csv:"-"`
	Output                          int64     `csv:"output"`
	Queue                           int64     `csv:"queue"`
	ExecutedTimeAvg                 float64   `csv:"executed_time_avg"`
	ExecutedTimeAvgSamples          []float64 `csv:"-"`
	ExecutedTimeBenchmarkAvg        float64   `csv:"executed_time_benchmark_avg"`
	ExecutedTimeBenchmarkAvgSamples []float64 `csv:"-"`
	ExecutedTotal                   int64     `csv:"executed_total"`
	CompleteLatency                 float64   `csv:"complete_latency"`
	PredictedInput                  int64     `csv:"predicted_input"`
	BoltsPredecessor                []string  `csv:"-"`
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

type Spout struct {
	Name string
}

type Topology struct {
	Id                 string
	Benchmark          bool
	InputRateAccum     int64
	InputRate          []int64
	PredictedInputRate []int64
	Bolts              []Bolt
	Spouts             []Spout
}

func (t *Topology) Init(id string) {
	t.Id = id
}

func (t *Topology) CreateTopology(summaryTopology SummaryTopology) {
	// Add Bolts
	for _, boltCurrent := range summaryTopology.Bolts {
		if !strings.Contains(boltCurrent.BoltID, "__") {
			var bolt = Bolt{
				Name:     boltCurrent.BoltID,
				Replicas: 1,
			}
			// Add bolts predecessor of current Bolt
			boltMetrics := GetComponentBolt(summaryTopology.Id, bolt.Name)
			// Waiting for the topology execution
			for len(boltMetrics.InputStats) == 0 {
				time.Sleep(200 * time.Millisecond)
				boltMetrics = GetComponentBolt(summaryTopology.Id, bolt.Name)
			}
			for i := range boltMetrics.InputStats {
				bolt.BoltsPredecessor = append(bolt.BoltsPredecessor, boltMetrics.InputStats[i].Component)
			}

			t.Bolts = append(t.Bolts, bolt)
		}
	}

	// Add Spouts
	for _, spoutCurrent := range summaryTopology.Spouts {
		var spout = Spout{
			Name: spoutCurrent.SpoutId,
		}
		t.Spouts = append(t.Spouts, spout)
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

func (t *Topology) InitReplicas() {
	for _, bolt := range t.Bolts {
		if errRedis := util.RedisSet(bolt.Name, strconv.FormatInt(1, 10)); errRedis != nil {
			log.Printf("init replicas error: %v\n", errRedis)
		}
	}
}

func (t *Topology) ClearStatsTimeWindow() {
	for i := range t.Bolts {
		t.Bolts[i].clearStatsTimeWindow()
	}
}

func (t *Topology) ClearQueue() {
	for i := range t.Bolts {
		t.Bolts[i].Queue = 0
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
