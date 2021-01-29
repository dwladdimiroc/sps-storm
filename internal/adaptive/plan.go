package adaptive

import "github.com/dwladdimiroc/sps-storm/internal/storm"

func planning(stateBolts map[string]int, topology *storm.Topology) {
	for nameBolt, stateBolt := range stateBolts {
		if stateBolt > 0 {
			addReplicaBolt(nameBolt, topology, int64(stateBolt))
		} else if stateBolt < 0 {
			removeReplicaBolt(nameBolt, topology, int64(stateBolt))
		}
	}

	determineEventLoss(topology)
}

func addReplicaBolt(nameBolt string, topology *storm.Topology, numberReplicas int64) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == nameBolt {
			topology.Bolts[i].Replicas += numberReplicas
		}
	}
}

func removeReplicaBolt(nameBolt string, topology *storm.Topology, numberReplicas int64) {
	for i := range topology.Bolts {
		if topology.Bolts[i].Name == nameBolt {
			if topology.Bolts[i].Replicas > 1 {
				topology.Bolts[i].Replicas += numberReplicas
			}
		}
	}
}

func determineEventLoss(topology *storm.Topology) {
	for i := range topology.Bolts {
		topology.Bolts[i].EventLoss = topology.Bolts[i].Queue
		topology.Bolts[i].EventLossAccum += topology.Bolts[i].Queue
		topology.Bolts[i].Queue = 0
	}
}
