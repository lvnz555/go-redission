package internal

import (
	"fmt"
	"log"
)

type LOGLEVEL uint8

const (
	DEBUG LOGLEVEL = iota + 1
	INFO
	WARN
	ERROR
	NOLOG
)

var Logger *log.Logger
var LogLevel = NOLOG

func Infof(s string, args ...interface{}) {
	if Logger == nil || LogLevel <= INFO {
		return
	}
	Logger.Output(2, fmt.Sprintf(s, args...))
}

func Debugf(s string, args ...interface{}) {
	if Logger == nil || LogLevel <= DEBUG {
		return
	}
	Logger.Output(2, fmt.Sprintf(s, args...))
}

func Warnf(s string, args ...interface{}) {
	if Logger == nil || LogLevel <= WARN {
		return
	}
	Logger.Output(2, fmt.Sprintf(s, args...))
}

func Errorf(s string, args ...interface{}) {
	if Logger == nil || LogLevel <= ERROR {
		return
	}
	Logger.Output(2, fmt.Sprintf(s, args...))
}
