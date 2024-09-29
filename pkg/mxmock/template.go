package mxmock

import (
	"strings"

	"github.com/ymatrix-data/mxbench/internal/util"
)

const (
	_LICENSE_TEMPLATE = "license_template" // obsoleted, please use _PLATE_TEMPLATE
	_PLATE_TEMPLATE   = "plate_template"
	_VIN_TEMPLATE     = "vin_template"

	_split_branch_template = "split_branch"

	// 'I','O','Q' is not allowed, see https://en.wikipedia.org/wiki/Vehicle_identification_number
	// Just a rough implementation, see https://github.com/yanigisawa/VinGenerator for a full version
	_VIN_CHARSET = "1234567890ABCDEFGHJKLMNPRSTUVWXYZ"
)

var loc = []string{
	"黑", "吉", "辽", "京", "津", "蒙", "鲁", "苏", "浙", "沪",
	"闽", "粤", "桂", "琼", "晋", "陕", "甘", "宁", "青", "新",
	"藏", "川", "云", "贵", "渝", "湘", "鄂", "赣", "豫", "港",
	"澳", "台", "皖", "冀",
}

var provinces = []string{"BJ", "TJ", "HE", "SX", "NM", "LN", "JL", "HL", "SH", "JS", "ZJ", "AH", "FJ", "JX", "SD", "HA", "HB", "HN", "GD", "GX", "HI", "CQ", "SC", "GZ", "YN", "XZ", "SN", "GS", "QH", "NX", "XJ"}

var validTemplateNames = []string{
	_LICENSE_TEMPLATE,
	_PLATE_TEMPLATE,
	_VIN_TEMPLATE,
	_split_branch_template,
}

func IsValidTemplateName(templateName string) bool {
	for _, validTemplateName := range validTemplateNames {
		if validTemplateName == templateName {
			return true
		}
	}
	return false
}

func GenerateValByTemplate(templateName string) string {
	switch templateName {
	case _LICENSE_TEMPLATE, _PLATE_TEMPLATE:
		return randomLicenseTemplate()
	case _VIN_TEMPLATE:
		return randomVinTemplate()
	case _split_branch_template:
		return randomSplitBranch()
	default:
	}

	return ""
}

func randomLicenseTemplate() string {
	var license [10]byte

	copy(license[:], loc[util.Intn(len(loc))])
	copy(license[3:], strings.ToUpper(util.String(7)))

	return string(license[:])
}

func randomVinTemplate() string {
	return util.StringWithCharset(17, _VIN_CHARSET)
}

func randomSplitBranch() string {
	var branch [9]byte
	copy(branch[:], provinces[util.Intn(len(provinces))])
	copy(branch[2:], "0000000")
	return string(branch[:])
}
