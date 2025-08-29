package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/glibtools/libs/config"
	"github.com/glibtools/libs/util"
)

var (
	BeforeStartFunc = config.LoadConfig

	StartFunc func()

	startCmd = &cobra.Command{
		Use: "start",
		Run: func(*cobra.Command, []string) {
			if BeforeStartFunc != nil {
				BeforeStartFunc()
			}
			if daemon {
				killProcess()
				cmdStart()
				return
			}
			if StartFunc != nil {
				StartFunc()
			}
		},
	}
)

func cmdStart() {
	file1name := filepath.Join(util.RootDir(), "logs", "runtime.log")
	_ = os.MkdirAll(filepath.Dir(file1name), 0755)
	file1, _ := os.OpenFile(file1name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	ec := os.Args[0]
	str := fmt.Sprintf("%s -x", ec)
	for _, v := range os.Args[1:] {
		str += fmt.Sprintf(" %s", v)
	}
	str = strings.Replace(str, "-d", "", 1)
	cmd := StdExec(str)
	cmd.Stdout = file1
	cmd.Stderr = file1
	_ = cmd.Start()
	log.Println("Start daemon success")
}

func killProcess() {
	ec := os.Args[0]
	ec = filepath.Base(ec)
	process := fmt.Sprintf("%s -x start", ec)
	_ = KillProcess(process)
	for {
		if !ProcessIsRunning(process) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Println("Stop daemon success")
}
