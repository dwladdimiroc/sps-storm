package main

import "github.com/dwladdimiroc/sps-storm/stats"

const NAME_APP = "testApp"
const DURATION = 1000

func main() {
	//Create instance VM in GCP
	//function createInstance()

	//Collect stats as CPU/RAM/Bandwidth
	stats.Collect(NAME_APP, DURATION)

	//Deploy app
	//function deployApp

	//Execute monitor
	//function executorMonitor

	//Finish program
	// function finishProgram
}
