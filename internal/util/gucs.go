package util

import (
	"strings"
)

// Values on all segments are consistent
// GUC          : gp_vmem_protect_limit
// Master  value: 4096
// Segment value: 8192
func ShowGUC(gucName string) (string, string, error) {
	stdoutString, err := runCmdAndDealingWithError(GP_CONFIG_CLI_BIN, "-s", gucName)
	if err != nil {
		return "", "", err
	}
	var masterValue, segmentsValue string
	stdoutLines := strings.Split(stdoutString, "\n")
	for _, line := range stdoutLines {
		if strings.HasPrefix(line, "Master  value: ") {
			masterValue = strings.Split(line, "Master  value: ")[1]
			continue
		}
		if strings.HasPrefix(line, "Segment value: ") {
			segmentsValue = strings.Split(line, "Segment value: ")[1]
			continue
		}
	}

	return masterValue, segmentsValue, nil
}

func SetGUC(gucName, gucValueOnMaster, gucValueOnSegments string) error {
	_, err := runCmdAndDealingWithError(GP_CONFIG_CLI_BIN, "-c", gucName, "-m", gucValueOnMaster, "-v", gucValueOnMaster, "--skipvalidation")
	return err
}
