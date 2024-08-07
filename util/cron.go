package util

import (
	"time"

	"github.com/robfig/cron/v3"
)

type Cron struct {
	*cron.Cron
}

func (c *Cron) Constructor() {
	c.Cron = cron.New(cron.WithSeconds(), cron.WithLocation(time.Local))
	c.Start()
}
