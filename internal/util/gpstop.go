package util

func RestartDB() error {
	_, err := runCmdAndDealingWithError(GP_STOP_CLI_BIN, "-iraq")
	return err
}
