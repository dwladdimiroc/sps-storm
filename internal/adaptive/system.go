package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/jasonlvhit/gocron"
	"github.com/spf13/viper"
	"time"
)

var topology *storm.Topology
var period int
var schedulerAdaptive *gocron.Scheduler

func Init(topologyId string) {
	topology = new(storm.Topology)
	topology.Id = topologyId
	summaryTopology := storm.GetSummaryTopology(topology.Id)
	topology.CreateTopology(summaryTopology)
	schedulerAdaptive = gocron.NewScheduler()
}

func Start(limit time.Duration) {
	go func(schedulerAdaptive *gocron.Scheduler) {
		schedulerAdaptive.Every(uint64(viper.GetInt("storm.adaptive.time_window_size"))).Seconds().Do(adaptiveSystem, topology)
		<-schedulerAdaptive.Start()
	}(schedulerAdaptive)
	time.Sleep(limit)
}

func adaptiveSystem(topology *storm.Topology) {
	if ok := monitor(topology.Id, topology); ok {
		if viper.GetBool("storm.deploy.analyze") {
			if period%viper.GetInt("storm.adaptive.logical.reactive.number_samples") == 0 {
				//analyze(topology)
				planning(topology)
				execute(*topology)
			}
		}
	}
	topology.ClearStatsTimeWindow()
}

func Stop() {
	schedulerAdaptive.Clear()
}
