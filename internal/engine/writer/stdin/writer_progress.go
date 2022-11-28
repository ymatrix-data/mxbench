package stdin

type WriterProgress struct {
	Start                   string `json:"start"`
	End                     string `json:"end"`
	Period                  string `json:"period"`
	CurrTotalRows           int64  `json:"currTotalRows"`
	TotalRows               int64  `json:"totalRows"`
	CurrPeriodRows          int64  `json:"currPeriodRows"`
	CurrTotalBytes          int64  `json:"currTotalBytes"`
	TotalBytes              int64  `json:"totalBytes"`
	CurrPeriodBytes         int64  `json:"currPeriodBytes"`
	WrittenMxgateTotal      int64  `json:"writtenMxGateTotal"`
	CurrPeriodWrittenMxgate int64  `json:"currPeriodWrittenMxGate"`
	TableSize               int64  `json:"tableSize"`
}
