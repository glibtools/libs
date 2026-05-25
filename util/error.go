package util

import (
	"strings"
)

type errorGroup []error

func (e errorGroup) Error() string {
	var s1 []string
	for _, e1 := range e {
		s1 = append(s1, e1.Error())
	}
	return strings.Join(s1, ",")
}

func NewGroupError(group []error) error {
	var e errorGroup
	for _, e1 := range group {
		if e1 != nil {
			e = append(e, e1)
		}
	}
	if len(e) == 0 {
		return nil
	}
	return e
}
