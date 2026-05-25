package cmd

import (
	"github.com/spf13/cobra"
)

var (
	BeforeStopFunc func()

	stopCmd = &cobra.Command{
		Use: "stop",
		Run: func(*cobra.Command, []string) {
			if BeforeStopFunc != nil {
				BeforeStopFunc()
			}
			killProcess()
		},
	}
)
