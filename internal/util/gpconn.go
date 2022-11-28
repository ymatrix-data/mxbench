package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
)

type DBConnParams struct {
	MasterPort     int    `mapstructure:"db-master-port"`
	MaxConnections int    `mapstructure:"db-max-conn"`
	MasterHost     string `mapstructure:"db-master-host"`
	User           string `mapstructure:"db-user"`
	Database       string `mapstructure:"db-database"`
	Password       string `mapstructure:"db-password"`
	Options        []string
}

func (params *DBConnParams) GetConnStr() string {
	krbSrvStr := ""
	krbSrvName, ok := os.LookupEnv("PGKRBSRVNAME")
	if ok {
		krbSrvStr = fmt.Sprintf(" krbsrvname='%s'", krbSrvName)
	}

	var options []string
	options = append(options, "-c optimizer=off")
	options = append(options, "-c gp_autostats_mode=none")
	for _, opt := range params.Options {
		options = append(options, "-c "+opt)
	}
	optStr := fmt.Sprintf("options='%s' ", strings.Join(options, " "))

	pwdStr := ""
	if params.Password != "" {
		pwdStr = fmt.Sprintf(" password='%s'", params.Password)
	}

	hostStr := params.MasterHost
	portStr := strconv.Itoa(params.MasterPort)

	return fmt.Sprintf(`%s%s user='%s'%s dbname='%s' host=%s port=%s sslmode=disable application_name=mxbench`, optStr, krbSrvStr, params.User, pwdStr, params.Database, hostStr, portStr)
}

// CreateDBConnection establishes a connection to YMatrix
// Call Close() after use
func CreateDBConnection(params DBConnParams) (*sqlx.DB, error) {
	connStr := params.GetConnStr()
	conn, err := sqlx.Connect("pgx", connStr)
	if err != nil {
		return nil, err
	}
	conn.SetMaxIdleConns(1)
	conn.SetMaxOpenConns(1)
	return conn, nil
}
