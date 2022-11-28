package util

import (
	"regexp"
	"strings"

	"github.com/blang/semver"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	MAJOR_VERSION_5 uint64 = 5
)

type DBVersion struct {
	VersionString string
	SemVer        semver.Version
}

func GetMXDBVersionFromDB(params DBConnParams) (DBVersion, error) {
	conn, err := CreateDBConnection(params)
	if err != nil {
		return DBVersion{}, err
	}
	defer conn.Close()
	var versionStr string
	err = conn.Get(&versionStr, "SELECT pg_catalog.version() AS versionstring")
	if err != nil {
		return DBVersion{}, err
	}
	return NewMXDBVersion(versionStr)
}

func NewMXDBVersion(version string) (DBVersion, error) {
	var dbversion DBVersion
	var err error
	dbversion.VersionString = version
	if !strings.Contains(dbversion.VersionString, "YMatrix") {
		return dbversion, err
	}

	versionStart := strings.Index(dbversion.VersionString, "(YMatrix ") + len("(YMatrix ")
	versionEnd := strings.Index(dbversion.VersionString, ")")
	if versionStart >= 0 && versionEnd > versionStart {
		dbversion.VersionString = dbversion.VersionString[versionStart:versionEnd]
	}
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(dbversion.VersionString)[0]
	dbversion.SemVer, err = semver.Make(threeDigitVersion)
	return dbversion, err
}
