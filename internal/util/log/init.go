package log

import (
	"strings"
)

func InitLogger(logLevel, appName string) error {
	opts := make([]Option, 0)

	// set log level
	fileLogLevel, stdLogLevel := LOGINFO, LOGINFO

	switch strings.ToLower(logLevel) {
	case "error":
		fileLogLevel, stdLogLevel = LOGERROR, LOGERROR
	case "verbose":
		fileLogLevel, stdLogLevel = LOGVERBOSE, LOGVERBOSE
	case "debug":
		fileLogLevel, stdLogLevel = LOGDEBUG, LOGDEBUG
	default:

	}

	// set log level
	opts = append(opts, WithFileLevel(fileLogLevel), WithStdLevel(stdLogLevel))
	// also print log to std
	opts = append(opts, WithAlsoToStd())
	// print colorful log tags into std
	opts = append(opts, WithColor())

	err := Init(appName, opts...)
	if err != nil {
		return err
	}

	return nil
}
