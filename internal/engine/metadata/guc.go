package metadata

import (
	"fmt"

	"github.com/ymatrix-data/mxbench/internal/util"
)

const (
	_MARS2_GUC_PREFIX_BEFORE_MAJOR_VERSION_5 = "sortheap_"
	_MARS2_GUC_PREFIX_AT_OR_AFTER_MAJOR_5    = "mars2_"
)

type GUCs []*GUC

type GUC struct {
	Name            string
	ValueOnMaster   string
	ValueOnSegments string
}

func (g *GUC) String() string {
	return fmt.Sprintf("GUC          : %s\n"+
		"Master  value: %s\n"+
		"Segment value: %s",
		g.Name,
		g.ValueOnMaster,
		g.ValueOnSegments)
}

func NewGUCForBothRoles(name, value string) *GUC {
	return &GUC{
		Name:            name,
		ValueOnMaster:   value,
		ValueOnSegments: value,
	}
}

func NewGUC(name, masterValue, segmentValue string) *GUC {
	return &GUC{
		Name:            name,
		ValueOnMaster:   masterValue,
		ValueOnSegments: segmentValue,
	}
}

func NewGUCs(cfg *Config) GUCs {
	//TODO: decide value according to cfg
	mars2Prefix := _MARS2_GUC_PREFIX_AT_OR_AFTER_MAJOR_5
	if cfg.DBVersion.SemVer.Major < util.MAJOR_VERSION_5 {
		mars2Prefix = _MARS2_GUC_PREFIX_BEFORE_MAJOR_VERSION_5
	}
	return GUCs{
		NewGUCForBothRoles("optimizer", "off"),
		NewGUCForBothRoles("resource_scheduler", "off"),
		NewGUCForBothRoles("max_stack_depth", "4MB"),
		NewGUCForBothRoles("gp_autostats_mode", "none"),
		NewGUCForBothRoles("gp_interconnect_type", "udpifc"),
		NewGUCForBothRoles("mx_interconnect_compress", "on"),
		NewGUCForBothRoles("gp_snapshotadd_timeout", "30s"),

		NewGUCForBothRoles("log_statement", "none"),
		NewGUCForBothRoles("log_checkpoints", "on"),
		NewGUCForBothRoles("log_min_messages", "warning"),
		NewGUCForBothRoles("log_rotation_size", "200MB"),
		NewGUCForBothRoles("log_min_duration_statement", "2s"),

		NewGUCForBothRoles(mars2Prefix+"rowcompress", "off"),
		NewGUCForBothRoles(mars2Prefix+"debug_pages", "off"),
		NewGUCForBothRoles(mars2Prefix+"debug_merge", "on"),
		NewGUCForBothRoles(mars2Prefix+"insertdelay_ms", "1000"),
		NewGUCForBothRoles(mars2Prefix+"automerge_threshold", "32"),
		NewGUCForBothRoles(mars2Prefix+"insertdelay_threshold", "600"),

		NewGUCForBothRoles("wal_keep_segments", "32"),
		NewGUCForBothRoles("max_wal_size", "4GB"),
		NewGUCForBothRoles("debug_walrepl_syncrep", "off"),
	}
}

func (meta *Metadata) GetGUCs() string {
	return meta.GUCs.SetGUCsCommand()
}

func (gs GUCs) SetGUCsCommand() string {
	setGUCsCommand := ""
	for _, guc := range gs {
		setGUCsCommand += fmt.Sprintf(
			"%s -c %s -m %s -v %s --skipvalidation\n",
			util.GP_CONFIG_CLI_BIN,
			guc.Name,
			guc.ValueOnMaster,
			guc.ValueOnSegments)
	}
	return setGUCsCommand
}

func (gs GUCs) String() string {
	outputStr := ""
	for _, g := range gs {
		outputStr += fmt.Sprintf("%s\n\n", g)
	}
	return outputStr
}
