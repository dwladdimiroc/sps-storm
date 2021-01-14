package app

import "github.com/dwladdimiroc/stats-storm/exec"

func Deploy() {
	appCmdStormApp := "sh"
	argsCmdStormApp := []string{"startApp.sh"}
	dirCmdStormApp := "/home/daniel/storm/projects"
	exec.Execute(appCmdStormApp, argsCmdStormApp, dirCmdStormApp)
}
