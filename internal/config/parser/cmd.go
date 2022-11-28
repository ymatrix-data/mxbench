package parser

import (
	"errors"
)

/**
 * Parse command from cli into configuration struct
 */
type CmdParser struct {
	*ENVParser

	isCmdParsed bool
}

func newCmdParser(envParser *ENVParser) *CmdParser {
	return &CmdParser{
		ENVParser: envParser,
	}
}

func (parser *CmdParser) parse() error {
	if !parser.isENVParsed {
		return errors.New("need env parsed")
	}
	defer func() { parser.isCmdParsed = true }()

	// Directly parse CLI arguments
	// Some of commands can be handled here
	cfg := parser.cfg
	args := parser.mainFlagSet.Args()
	configWanted := false
	if len(args) > 0 {
		switch args[0] {
		case "version":
			cfg.GlobalCfg.VersionWanted = true
		case "help":
			cfg.GlobalCfg.HelpWanted = true
		case "config":
			cfg.GlobalCfg.Command = "config"
			if cfg.GlobalCfg.CfgFile != "" {
				return errIncorrectUsage
			}
			configWanted = true
		case "run":
			// Run is the default behavior that start the bench in current session
		default:
			return errUnknown
		}

		if configWanted {
			return errConfigWanted
		}

		cfg.GlobalCfg.Command = args[0]
	}

	if cfg.GlobalCfg.HelpWanted {
		return errHelpWanted
	}

	if cfg.GlobalCfg.VersionWanted {
		return errVersionWanted
	}
	return nil
}
