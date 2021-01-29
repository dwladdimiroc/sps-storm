package adaptive

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"log"
	"strings"
)

func execute(topology storm.Topology) {
	params := parseParamsRebalanced(topology)
	ok := storm.Rebalanced(topology.Id, params)
	log.Printf("execute: rebalanced topolog %v\n", ok)
	period = 0
}

func parseParamsRebalanced(topology storm.Topology) string {
	var base = "{\"rebalanceOptions\" : {\"executors\" : {TOPOLOGY_STORM}}, \"callback\" : \"foo\"}"
	var executors string
	for i, bolt := range topology.Bolts {
		var paramBolt string
		if i < len(topology.Bolts)-1 {
			paramBolt = fmt.Sprintf("\"%s\":%d,", bolt.Name, bolt.Replicas)
		} else {
			paramBolt = fmt.Sprintf("\"%s\":%d", bolt.Name, bolt.Replicas)
		}
		executors += paramBolt
	}
	params := strings.Replace(base, "TOPOLOGY_STORM", executors, 1)
	return params
}
