package util

import "fmt"

func RestartDB() error {
	outStr, err := runCmdAndDealingWithError(PSQL_BIN, "-c", "show mx_ha_provider")
	if err != nil || outStr == "" {
		return fmt.Errorf("fail to show mx_ha_provider %v", err)
	}
	var STOP_CLI_BIN string
	if outStr == "external" {
		STOP_CLI_BIN = MX_STOP_CLI_BIN
	} else {
		STOP_CLI_BIN = GP_STOP_CLI_BIN
	}
	_, err = runCmdAndDealingWithError(STOP_CLI_BIN, "-iraq")
	return err
}
