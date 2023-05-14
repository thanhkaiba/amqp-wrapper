package amqpwrapper

import "log"

// LoggerAdapter is an interface, that you need to implement to support Watermill logging.
// You can use StdLoggerAdapter as a reference implementation.
type LoggerAdapter interface {
	Error(format string, v ...interface{})
	Info(format string, v ...interface{})
	Debug(format string, v ...interface{})
}

type DefaultLogger struct {
}

func (DefaultLogger) Error(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (DefaultLogger) Info(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (DefaultLogger) Debug(format string, v ...interface{}) {
	log.Printf(format, v)
}
