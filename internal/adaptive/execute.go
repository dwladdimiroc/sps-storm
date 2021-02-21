package adaptive

import (
	"fmt"
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"log"
	"strconv"
	"strings"
)

func execute(topology storm.Topology) {
	//params := parseParamsRebalanced(topology)
	//ok := storm.Rebalanced(topology.Id, params)
	err := updateReplicas(topology)
	log.Printf("execute: rebalanced topolog %v\n", err)
	period = 0
}

func parseParamsRebalanced(topology storm.Topology) string {
	var base = "{\"rebalanceOptions\" : {\"executors\" : {TOPOLOGY_Sfor i, bolt := range topology.Bolts {TORM}}, \"callback\" : \"foo\"}"
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

func updateReplicas(topology storm.Topology) error {
	var err error
	for _, bolt := range topology.Bolts {
		value := strconv.FormatInt(bolt.Replicas, 10)
		if errRedis := util.RedisSet(bolt.Name, value); errRedis != nil {
			log.Printf("update replicas error: %v\n", errRedis)
			err = errRedis
		}
	}

	return err
}
