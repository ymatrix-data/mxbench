package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util"
	"github.com/ymatrix-data/mxbench/internal/util/log"
	"github.com/ymatrix-data/mxbench/internal/util/mxerror"
	"github.com/ymatrix-data/mxbench/pkg/mxmock"
)

const (
	NoticeColor  = "\033[1;36m%s\033[0m"
	WarningColor = "\033[1;33m%s\033[0m"
)

const (
	_SELECT_VIN_VALUES_SQL = `SELECT distinct(%s) AS vin FROM %s`
)

const watchInterval = time.Second * 5

type ExecDDLFunc func() error

type ExecSetGUCsFunc func() error

type NilCheck interface {
	IsNil() bool
}

type IEngine interface {
	NilCheck
	Run() error
	Close() error
	PrintStat()
	PrintProgress()
	GetFormattedSummary()
}

type Engine struct {
	IGenerator IGenerator
	IWriter    IWriter
	IBenchmark IBenchmark
	Metadata   *metadata.Metadata
	VolumeDesc VolumeDesc

	Config *Config

	ctx            context.Context
	cancelFunc     context.CancelFunc
	watchWaitGroup sync.WaitGroup

	execDDLFunc     ExecDDLFunc
	execSetGUCsFunc ExecSetGUCsFunc
	writeFunc       WriteFunc
	execBenchFunc   ExecBenchFunc

	ddlFile   *os.File
	dataFile  *os.File
	benchFile *os.File

	gucSetupFile  *os.File
	gucBackupFile *os.File

	workspace string

	oldGUCs metadata.GUCs
	newGUCs metadata.GUCs
}

var _ IEngine = (*Engine)(nil)

func NewEngineFromConfig(cfg *Config) (*Engine, error) {
	e := &Engine{
		Config:     cfg,
		IGenerator: cfg.NewGeneratorFunc(cfg.GeneratorCfg),
		IBenchmark: cfg.NewBenchmarkFunc(cfg.BenchmarkCfg),
		IWriter:    cfg.NewWriterFunc(cfg.WriterCfg),
	}

	e.ctx, e.cancelFunc = context.WithCancel(context.Background())

	e.writeFunc = e.IWriter.Write
	e.execSetGUCsFunc = e.execSetGUCs
	e.execBenchFunc = e.execBench

	err := e.prepareWorkspace()
	if cfg.GlobalCfg.Dump {
		e.writeFunc = e.dumpData
		e.execDDLFunc = e.dumpDDL
		e.execBenchFunc = e.dumpBench
		e.execSetGUCsFunc = e.dumpSetGUCs
	}

	if cfg.GlobalCfg.DDLFilePath != "" {
		e.execDDLFunc = e.execDDLFromFile
	}

	return e, err
}

func (e *Engine) getVinValuesFromTable() ([]string, error) {
	vinCol := e.Metadata.Table.Columns[metadata.VINColumnIndex]

	conn, err := util.CreateDBConnection(e.Config.DB)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	sql := fmt.Sprintf(_SELECT_VIN_VALUES_SQL, vinCol.Name, e.Metadata.Table.Identifier())
	vinValues := make([]string, 0, e.Metadata.Cfg.TagNum)
	if err := conn.Select(&vinValues, sql); err != nil {
		return nil, err
	}

	return vinValues, nil
}

func (e *Engine) generateVinVals() ([]string, error) {
	vinVals := make([]string, 0, e.Metadata.Cfg.TagNum)

	if len(e.Metadata.Table.ColumnSpecs) < metadata.VINColumnIndex+1 {
		return vinVals, nil
	}

	columnSpec := e.Metadata.Table.ColumnSpecs[metadata.VINColumnIndex]
	if columnSpec != nil && mxmock.IsValidTemplateName(columnSpec.Name) {
		for idx := int64(0); idx < e.Config.GlobalCfg.TagNum; idx++ {
			vinVal := mxmock.GenerateValByTemplate(columnSpec.Name)
			vinVals = append(vinVals, vinVal)
		}
	} else {
		for idx := int64(0); idx < e.Config.GlobalCfg.TagNum; idx++ {
			vinVals = append(vinVals, strconv.FormatInt(idx, 10))
		}
	}

	return vinVals, nil
}

func (e *Engine) makeWorkspaceDir() error {
	dir := e.Config.GlobalCfg.Workspace
	dir = filepath.Join(dir, fmt.Sprintf("%d", time.Now().Unix()))
	e.workspace = dir
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return mxerror.CommonErrorf("workspace(%s) is a file", dir)
		}
		return nil
	}
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, os.ModePerm)
	}
	return err
}

func (e *Engine) prepareWorkspace() error {
	err := e.makeWorkspaceDir()
	if err != nil {
		return err
	}
	return e.openFiles()
}

func (e *Engine) openFiles() error {
	var err error
	dir := e.workspace

	if e.Config.GlobalCfg.DDLFilePath == "" {
		e.ddlFile, err = os.OpenFile(filepath.Join(dir, "mxbench_ddl.sql"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
	}

	if !e.Config.GlobalCfg.SkipSetGUCs {
		e.gucSetupFile, err = os.OpenFile(filepath.Join(dir, "mxbench_gucs_setup.sh"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			return err
		}
	}

	bfn := fmt.Sprintf("mxbench_%s_query.sql", e.Config.BenchmarkCfg.Plugin)
	e.benchFile, err = os.OpenFile(filepath.Join(dir, bfn), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	if !e.Config.GlobalCfg.Dump {
		return nil
	}

	dafn := fmt.Sprintf("mxbench_%s_data.csv", e.Config.GeneratorCfg.Plugin)
	e.dataFile, err = os.OpenFile(filepath.Join(dir, dafn), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) closeFiles() error {
	if e.Config.GlobalCfg.DDLFilePath == "" {
		if err := e.ddlFile.Close(); err != nil {
			return err
		}
	}

	if !e.Config.GlobalCfg.SkipSetGUCs {
		if err := e.gucSetupFile.Close(); err != nil {
			return err
		}
	}

	if err := e.benchFile.Close(); err != nil {
		return err
	}

	if !e.Config.GlobalCfg.Dump {
		return nil
	}

	return e.dataFile.Close()
}

func (e *Engine) PrintStat() {
	if e.Config.GlobalCfg.Dump {
		return
	}

	if stat := e.IWriter.GetStat(); stat != nil {
		fmt.Println(stat.GetSummary())
	}

	if stat := e.IBenchmark.GetStat(); stat != nil {
		fmt.Println(stat.GetSummary())
	}
}

func (e *Engine) GetFormattedSummary() {
	if e.Config.GlobalCfg.Dump {
		return
	}
	var row, prefix string
	if stat := e.IWriter.GetStat(); stat != nil {
		prefix = stat.GetFormattedSummary("")
		prefix += util.DELIMITER
	}
	if stat := e.IBenchmark.GetStat(); stat != nil {
		row = stat.GetFormattedSummary(prefix)
	}
	// writer row to file
	switch e.Config.GlobalCfg.ReportFormat {
	case ReportFormatCSV:
		filePath := filepath.Join(e.Config.GlobalCfg.ReportPath, "report.csv")
		file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Warn("Report directory open failed: %v", err)
		}
		defer file.Close()
		_, err = file.WriteString(row)
		if err != nil {
			log.Warn("Write report failed: %v", err)
		}
	default:
		return
	}
}

func (e *Engine) PrintProgress() {
	if e.Config.GlobalCfg.Dump {
		return
	}

	if stat := e.IWriter.GetStat(); stat != nil {
		fmt.Println(stat.GetProgress())
	}

	if stat := e.IBenchmark.GetStat(); stat != nil {
		fmt.Println(stat.GetProgress())
	}
}

func (e *Engine) Watch() {
	defer e.watchWaitGroup.Done()
	if e.Config.GlobalCfg.Dump || !e.Config.GlobalCfg.Watch {
		return
	}

	tick := time.NewTicker(watchInterval)
	for {
		select {
		case <-tick.C:
			e.PrintProgress()
		case <-e.ctx.Done():
			e.PrintProgress()
			return
		}
	}
}

func (e *Engine) Run() error {
	err := util.CreateDBIfNotExists(e.Config.DB)
	if err != nil {
		return err
	}

	if e.Config.GlobalCfg.DDLFilePath == "" {
		metaConfig, err := e.newMetadataConfig()
		if err != nil {
			return err
		}
		e.Metadata, err = metadata.New(metaConfig)
		if err != nil {
			return err
		}
		// TODO: reset is quite redundant
		e.Config.GlobalCfg.TotalMetricsCount = metaConfig.TotalMetricsCount
		if !e.Config.GlobalCfg.Dump {
			e.execDDLFunc = e.execDDL
		}
	}

	// no writing data
	noWirterOrGenerator := e.Config.GeneratorCfg.Plugin == "nil" || e.Config.WriterCfg.Plugin == "nil"
	qeuryOnlyMode := noWirterOrGenerator && !e.Config.GlobalCfg.Dump
	if qeuryOnlyMode {
		e.execDDLFunc = func() error { return nil }
	}

	log.Info("Begin to execute DDL")
	err = e.execDDLFunc()
	if err != nil {
		return err
	}
	log.Info("DDL executed successfully")

	// after having executed DDL
	// initiate metadata
	if e.Config.GlobalCfg.DDLFilePath != "" {
		metaConfig, err := e.newMetadataConfig()
		if err != nil {
			return err
		}
		e.Metadata, err = metadata.New(metaConfig)
		if err != nil {
			return err
		}
	}

	if qeuryOnlyMode {
		// When no writing data, get vin values from dest table
		e.Metadata.Table.VinValues, err = e.getVinValuesFromTable()
		if err != nil {
			return err
		}
	} else {
		// When writing data, generate data in advance.
		e.Metadata.Table.VinValues, err = e.generateVinVals()
		if err != nil {
			return err
		}
	}

	// initiate prediction
	e.VolumeDesc.GeneratorPrediction, err = e.IGenerator.GetPrediction(e.Metadata.Table)
	if err != nil {
		return err
	}
	e.VolumeDesc.GetTableSizeFunc = e.getTableSize

	err = e.handleGUCs()
	if err != nil {
		return err
	}

	writerFinCh, err := func() (<-chan error, error) {
		if e.Config.GlobalCfg.Dump {
			ch := make(chan error)
			close(ch)
			return ch, nil
		}
		return e.IWriter.Start(*e.Config, e.VolumeDesc)
	}()
	if err != nil {
		return err
	}

	benchmarkFinCh := make(chan error, 1)
	go func() {
		defer close(benchmarkFinCh)
		if !e.Config.GlobalCfg.SimultaneousLoadAndQuery {
			err := <-writerFinCh
			if err != nil {
				return
			}
		}
		log.Info("Begin to run benchmark queries")
		if err := e.IBenchmark.Run(writerFinCh, *e.Config, e.Metadata, e.execBenchFunc); err != nil {
			benchmarkFinCh <- err
			return
		}
		log.Info("Benchmark queries done")
	}()

	e.watchWaitGroup.Add(1)
	go e.Watch()

	if err := e.IGenerator.Run(e.Config.GlobalCfg, e.Metadata, e.writeFunc); err != nil {
		return err
	}

	if err := e.IWriter.WriteEOF(); err != nil {
		return err
	}

	if e.Config.GlobalCfg.SimultaneousLoadAndQuery {
		err = <-writerFinCh
		if err != nil {
			return err
		}
	}

	return <-benchmarkFinCh
}

func (e *Engine) newMetadataConfig() (*metadata.Config, error) {
	metaConfig := e.Config.GlobalCfg.NewMetadataConfig()
	e.IGenerator.ModifyMetadataConfig(metaConfig)
	metaConfig.DB = e.Config.DB
	var err error
	metaConfig.DBVersion, err = util.GetMXDBVersionFromDB(metaConfig.DB)
	return metaConfig, err
}

func (e *Engine) handleGUCs() error {
	if e.Config.GlobalCfg.SkipSetGUCs {
		log.Info("Skip to execute setting GUCs")
		return nil
	}

	err := e.backupGUCs()
	if err != nil {
		return err
	}

	log.Info("Begin to execute setting GUCs")
	err = e.execSetGUCsFunc()
	if err != nil {
		return err
	}
	log.Info("GUCs setting executed successfully")

	return nil
}

func (e *Engine) Close() error {
	defer func() {
		e.cancelFunc()
		e.watchWaitGroup.Wait()
	}()
	if err := e.safeCloseGenerator(); err != nil {
		return err
	}

	if err := e.safeCloseBenchmark(); err != nil {
		return err
	}

	if err := e.closeFiles(); err != nil {
		return err
	}

	if !e.Config.GlobalCfg.Dump {
		// Needn't to stop writer when dump has setted
		return e.safeStopWriter()
	}
	return nil

}

func (e *Engine) safeCloseGenerator() error {
	if e.IGenerator.IsNil() {
		return nil
	}
	return e.IGenerator.Close()
}

func (e *Engine) safeStopWriter() error {
	if e.IWriter.IsNil() {
		return nil
	}
	return e.IWriter.Stop()
}

func (e *Engine) safeCloseBenchmark() error {
	if e.IBenchmark.IsNil() {
		return nil
	}
	return e.IBenchmark.Close()
}

func (e *Engine) dumpData(msg []byte, _, _ int64) error {
	_, err := e.dataFile.Write(msg)
	return err
}

func (e *Engine) execBench(gCtx context.Context, query Query, stat Stat) error {
	err := e.dumpBench(gCtx, query, stat)
	if err != nil {
		return err
	}
	ebs, ok := stat.(*ExecBenchStat)
	if !ok {
		return mxerror.CommonErrorf("stat type conversion error")
	}
	ctx, cancel := context.WithCancel(gCtx)
	opt := ebs.opt

	runTimes := opt.RunTimes
	if runTimes <= 0 {
		if opt.Duration > 0 {
			ctx, cancel = context.WithTimeout(ctx, opt.Duration)
		}
	}
	defer cancel()

	connPool := make([]*sqlx.DB, 0, opt.Parallel)
	for p := 0; p < opt.Parallel; p++ {
		conn, err := util.CreateDBConnection(e.Config.DB)
		if err != nil {
			log.Error("db connection create error: %v", err)
			return err
		}
		connPool = append(connPool, conn)
	}

	var wg sync.WaitGroup
	wg.Add(opt.Parallel)
	start := time.Now()

	for j := 0; j < opt.Parallel; j++ {
		go func(ctx context.Context, conn *sqlx.DB) {
			defer func() {
				wg.Done()
				conn.Close()
			}()
			var runs int64
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if runTimes > 0 && runs >= runTimes {
					break
				}
				singleQueryStart := time.Now()
				_, err := conn.Exec(query.GetSQL())
				ebs.addLatency(time.Since(singleQueryStart))
				atomic.AddInt64(&ebs.runs, 1)
				atomic.StoreInt64(&ebs.TimeElapsed, int64(time.Since(start)))
				if err != nil {
					log.Error("query: %s execute error: %v", query.GetName(), err)
					return
				}
				runs++
			}
		}(ctx, connPool[j])
	}
	wg.Wait()
	atomic.StoreInt64(&ebs.TimeElapsed, int64(time.Since(start)))

	return nil
}

func (e *Engine) dumpBench(_ context.Context, query Query, _ Stat) error {
	_, err := e.benchFile.WriteString(fmt.Sprintf("-- query name: %s\n%s;\n", query.GetName(), query.GetSQL()))
	return err
}

func (e *Engine) execDDL() error {
	ddl := e.Metadata.GetDDL()
	_, err := e.ddlFile.WriteString(ddl)
	if err != nil {
		return err
	}
	conn, err := util.CreateDBConnection(e.Config.DB)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.Exec(e.Metadata.GetDDL())
	return err
}

func (e *Engine) execDDLFromFile() error {
	ddlBytes, err := os.ReadFile(e.Config.GlobalCfg.DDLFilePath)
	if err != nil {
		return err
	}
	ddlFromFile := string(ddlBytes)
	conn, err := util.CreateDBConnection(e.Config.DB)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.Exec(ddlFromFile)
	return err
}

func (e *Engine) getTableSize() (int64, error) {
	var totalSize int64
	conn, err := util.CreateDBConnection(e.Config.DB)
	if err != nil {
		return totalSize, err
	}

	defer conn.Close()

	rows, err := conn.Query(e.Metadata.GetTableSizeSQL())

	if err != nil {
		return totalSize, err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&totalSize); err != nil {
			return totalSize, err
		}
	}
	return totalSize, err
}

func (e *Engine) dumpDDL() error {
	_, err := e.ddlFile.WriteString(e.Metadata.GetDDL())
	return err
}

func (e *Engine) dumpSetGUCs() error {
	_, err := e.gucSetupFile.WriteString(e.Metadata.GetGUCs())
	return err
}

func (e *Engine) execSetGUCs() error {
	err := e.dumpSetGUCs()
	if err != nil {
		return err
	}

	// check GUCs that need to be set
	if len(e.newGUCs) == 0 {
		log.Info("No need to set GUCs, skipping...")
		return nil
	}
	// ask for confirmation
	log.Info("Confirm setting GUCs:\n\n%s",
		e.newGUCs)
	fmt.Printf(WarningColor, fmt.Sprintf("The above %d GUC(s) are going to be set\n", len(e.newGUCs)))
	fmt.Printf(NoticeColor, "Continue setting GUCs and restarting YMatrix: Yy|Nn (default=N): ")
	var confirmStr string
	fmt.Scanln(&confirmStr)
	if strings.ToLower(strings.TrimSpace(confirmStr)) != "y" {
		// ask for permission to continue
		fmt.Printf(NoticeColor, "GUC setting aborted, continuing with current GUCs: Yy|Nn (default=N): ")
		fmt.Scanln(&confirmStr)
		if strings.ToLower(strings.TrimSpace(confirmStr)) != "y" {
			log.Info("user abort setting GUCs and restarting YMatrix, exitting...")
			return mxerror.CommonErrorf("abort setting GUCs and restarting YMatrix from user")
		}
		log.Info("user abort setting GUCs and restarting YMatrix, continuing...")
		return nil
	}

	// set GUCs
	log.Info("Setting GUCs and restarting YMatrix...")
	for _, g := range e.newGUCs {
		err := util.SetGUC(g.Name, g.ValueOnMaster, g.ValueOnSegments)
		if err != nil {
			return err
		}
	}

	// restart DBMS
	err = util.RestartDB()
	if err != nil {
		return err
	}
	fmt.Printf(WarningColor, fmt.Sprintf("Setting GUCs and restarting YMatrix completed, run %s and restart YMatrix to revert GUCs\n", e.gucBackupFile.Name()))
	return nil
}

// read user GUCs and backup
func (e *Engine) backupGUCs() error {
	oldGUCs := make(metadata.GUCs, 0)
	newGUCs := make(metadata.GUCs, 0)
	for _, g := range e.Metadata.GUCs {
		masterValue, segmentsValue, err := util.ShowGUC(g.Name)
		if err != nil {
			return err
		}
		if masterValue != g.ValueOnMaster || segmentsValue != g.ValueOnSegments {
			oldGUCs = append(oldGUCs, metadata.NewGUC(g.Name, masterValue, segmentsValue))
			newGUCs = append(newGUCs, g)
		}
	}

	if len(oldGUCs) <= 0 {
		return nil
	}

	var err error
	e.gucBackupFile, err = os.OpenFile(filepath.Join(e.workspace, "mxbench_gucs_backup.sh"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return err
	}
	defer func() {
		_ = e.gucBackupFile.Close()
	}()

	command := oldGUCs.SetGUCsCommand()
	_, err = e.gucBackupFile.WriteString(command)
	e.oldGUCs = oldGUCs
	e.newGUCs = newGUCs
	return err
}

func (e *Engine) IsNil() bool {
	return e == nil
}
