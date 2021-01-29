package app

import (
	"github.com/dwladdimiroc/sps-storm/internal/storm"
	"github.com/dwladdimiroc/sps-storm/internal/util"
	"github.com/spf13/viper"
)

const DirCmd = "scripts"

func Deploy() string {
	appCmdStormApp := "sh"
	argsCmdStormApp := []string{viper.GetString("storm.deploy.script")}
	dirCmdStormApp := DirCmd
	util.Execute(appCmdStormApp, argsCmdStormApp, dirCmdStormApp)
	topologyId := storm.GetTopologyId()
	return topologyId
}
