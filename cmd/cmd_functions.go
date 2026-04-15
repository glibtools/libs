package cmd

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cast"
)

// FindPidSliceByProcessName get a pid list
func FindPidSliceByProcessName(name string) []string {
	//ps -ef|grep -v grep|grep 'app'|awk '{print $2}'|tr -s '\n'
	str := `ps -ef|grep -v grep|grep '{name}'|awk '{print $2}'|tr -s '\n'`
	p, _ := StdExec(strings.Replace(str, "{name}", name, -1)).Output()
	ps := strings.Split(string(bytes.TrimSpace(p)), "\n")
	return ps
}

func FindPIDWithPort(port uint32) string {
	//str := `lsof -i:{port}|grep -v grep|awk '{print $2}'|tr -s '\n'`
	//str := `lsof -i:800|grep -v grep|awk '{print $2}'|awk 'NR==2{print}'|tr -s '\n'`
	str := `lsof -i:{port}|grep -v grep|awk '{print $2}'|awk 'NR==2{print}'|tr -s '\n'`
	str = strings.Replace(str, "{port}", cast.ToString(port), -1)
	p, _ := StdExec(str).Output()
	return string(bytes.TrimSpace(p))
}

// KillProcess ...kill process
func KillProcess(name string) (err error) {
	if !ProcessIsRunning(name) {
		return fmt.Errorf("process[%s] is not running", name)
	}
	ps := FindPidSliceByProcessName(name)
	for _, pid := range ps {
		KillProcessByPID(pid)
	}
	return
}

func KillProcessByPID(pid string) {
	execStr := fmt.Sprintf("kill %s", pid)
	log.Printf("kill process: %s", execStr)
	_ = StdExec(execStr).Run()
}

func KillProcessWithPort(port uint32) (err error) {
	pid := FindPIDWithPort(port)
	if len(pid) == 0 {
		return fmt.Errorf("process[%d] is not running", port)
	}
	KillProcessByPID(pid)
	tickUntilFunc(func() bool { return FindPIDWithPort(port) == "" })
	return
}

// ProcessIsRunning is running
func ProcessIsRunning(name string) bool {
	ps := FindPidSliceByProcessName(name)
	return len(ps) > 0 && len(ps[0]) > 0
}

func StdExec(script string) *exec.Cmd {
	return exec.Command("/bin/sh", "-c", script)
}

func tickUntilFunc(fn func() bool) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for range tk.C {
		if fn() {
			return
		}
	}
}
