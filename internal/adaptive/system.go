package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/jasonlvhit/gocron"
)

var topology *storm.Topology

func Init(topologyId string) {
	topology = new(storm.Topology)
	topology.Id = topologyId
	summaryTopology := storm.GetSummaryTopology(topology.Id)
	topology.CreateTopology(summaryTopology)
}

func Start() {
	schedulerAdaptive := gocron.NewScheduler()
	schedulerAdaptive.Every(5).Seconds().Do(UpdateStats, topology.Id, topology)
	<-schedulerAdaptive.Start()
}

func Stop() {

}
