package adaptive

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"log"
	"strconv"
)

func execute(topology storm.Topology) {
	err := updateReplicas(topology)
	log.Printf("execute: rebalanced topolog %v\n", err)
	period = 0
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
