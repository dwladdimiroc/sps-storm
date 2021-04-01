package main

import (
	"github.com/dwladdimiroc/sps-storm/internal/adaptive"
	"github.com/dwladdimiroc/sps-storm/internal/app"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
	"log"
	"time"
)

func main() {
	if err := util.LoadConfig(); err != nil {
		log.Panicf("error load config: %v\n", err)
	}

	//Create instance VM in GCP
	//function createInstance()

	//Collect stats as CPU/RAM/Bandwidth
	//stats.Collect(NAME_APP, DURATION)

	//Previous deploy
	//previousDeploy()

	//Deploy app
	topologyId := app.Deploy()

	//Execute adaptive
	adaptive.Init(topologyId)
	adaptive.Start(time.Duration(viper.GetInt("storm.deploy.duration")) * time.Minute)
	adaptive.Stop()

	//Finish program
	// function finishProgram
}

func previousDeploy() {
	if err := util.RedisFlush(); err != nil {
		log.Printf("redis error: %v\n", err)
	}
}
