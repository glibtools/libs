package util

import (
	"log"
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

func (c *Cron) MustAddFunc(spec string, fn func()) cron.EntryID {
	id, err := c.Cron.AddFunc(spec, fn)
	if err != nil {
		log.Fatalf("AddFunc error: %v", err)
	}
	return id
}

func (c *Cron) MustAddJob(spec string, cmd cron.Job) cron.EntryID {
	id, err := c.Cron.AddJob(spec, cmd)
	if err != nil {
		log.Fatalf("AddJob error: %v", err)
	}
	return id
}
