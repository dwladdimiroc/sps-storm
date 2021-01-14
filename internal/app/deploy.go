package app

import (
	"github.com/dwladdimiroc/sps-storm/internal/util"
)

func Deploy() {
	appCmdStormApp := "sh"
	argsCmdStormApp := []string{"startApp.sh"}
	dirCmdStormApp := "/home/daniel/storm/projects"
	util.Execute(appCmdStormApp, argsCmdStormApp, dirCmdStormApp)
}
