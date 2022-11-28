package parser

import (
	"errors"
	"os"
	"strconv"
)

/**
 * Parse env into configuration struct
 */
type ENVParser struct {
	// The tag map of fields which value read from env
	sourceEnvTag map[string]bool
	isENVParsed  bool
	*FlagsParser
}

func newEnvParser(flagsParser *FlagsParser) *ENVParser {
	return &ENVParser{
		FlagsParser:  flagsParser,
		sourceEnvTag: map[string]bool{},
	}
}

// Parse env to config, must run after ParseFlags, because this depend on flags
func (parser *ENVParser) parse() error {
	if !parser.isFlagParsed {
		return errors.New("need flags parsed")
	}
	defer func() { parser.isENVParsed = true }()
	parser.fillENV("PGDATABASE", "db-database")
	parser.fillENV("PGPORT", "db-master-port", func(env string) bool {
		pgPort, err := strconv.Atoi(env)
		if err != nil {
			return false
		}
		return pgPort > 0
	})
	return nil
}

func (parser *ENVParser) fillENV(envName, flagName string, filters ...func(env string) bool) {
	env := os.Getenv(envName)
	if env == "" {
		return
	}

	for _, filter := range filters {
		if !filter(env) {
			return
		}
	}

	flag := parser.findFlag(flagName)
	if flag == nil {
		return
	}

	if flag.Changed {
		return
	}

	err := flag.Value.Set(env)
	if err != nil {
		return
	}

	flag.Changed = true
	parser.sourceEnvTag[flagName] = true
}
