package util

func RestartDB() error {
	var STOP_CLI_BIN string
	STOP_CLI_BIN = GP_STOP_CLI_BIN
	_, err := runCmdAndDealingWithError(STOP_CLI_BIN, "-iraq")
	return err
}
