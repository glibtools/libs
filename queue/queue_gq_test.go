package queue

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/glibtools/libs/util"
)

type taskFailureLogger struct {
	util.ItfLogger
	lines []string
}

func (l *taskFailureLogger) Errorf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func TestTaskFailureLogsOnlyTypeAndID(t *testing.T) {
	logger := &taskFailureLogger{}
	worker := &gqWorker{logger: logger}
	task := GQueueTask{Name: "synthetic.type", ID: "synthetic-id", MaxRetry: 0, Data: []byte(`{"nick":"private-nick","score":12345}`)}
	worker.runTask(task, func(*GQueueTask) error { return errors.New("private-nick score=12345") })
	if len(logger.lines) != 1 {
		t.Fatalf("日志数=%d", len(logger.lines))
	}
	line := logger.lines[0]
	if !strings.Contains(line, task.Name) || !strings.Contains(line, task.ID) {
		t.Fatalf("缺少类型或 ID: %s", line)
	}
	for _, forbidden := range []string{"private-nick", "12345", "Data:", "score"} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("日志包含业务数据: %s", forbidden)
		}
	}
}
