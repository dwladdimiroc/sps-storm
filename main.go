package main

import (
	"github.com/dwladdimiroc/sps-storm/internal/adaptive"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"time"
)

const NAME_APP = "testApp"
const DURATION = 1 * time.Hour

func main() {
	util.LoadConfig()
	var topologyId = "syntheticApp-1-1610997255"
	adaptive.Init(topologyId)
	adaptive.Start()

	//Create instance VM in GCP
	//function createInstance()

	//Collect stats as CPU/RAM/Bandwidth
	//stats.Collect(NAME_APP, DURATION)

	//Deploy app
	//app.Deploy()

	//Execute adaptive
	//function executorMonitor

	//Finish program
	// function finishProgram
}
