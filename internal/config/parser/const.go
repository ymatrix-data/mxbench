package parser

import "github.com/ymatrix-data/mxbench/internal/util/mxerror"

var (
	errUnknown    = mxerror.CommonError("unknown")
	errParseFlags = mxerror.CommonError("parse flags error")

	errHelpWanted    = mxerror.SuccessError("help wanted")
	errConfigWanted  = mxerror.SuccessError("config wanted")
	errVersionWanted = mxerror.SuccessError("version wanted")

	errIncorrectUsage = mxerror.IncorrectUsageError("invalid usage: command config conflict with --config option")
)
