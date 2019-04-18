package res

import (
	"archive/tar"
	"bytes"
	"fmt"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/recover/bpct"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/client"
	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/utils"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
)

/***
data recovery
*/

// Recover 恢复
type Recover struct {
	Writer             io.Writer                 // properties writer
	Table              string                    // table name
	After              *final.After              // after match
	TempPath           string                    // TempPath 临时目录
	Client             inter.IClient             // Client 远程客户端
	End                int64                     // End time
	SqlExecutor        *db.MetaConf              // SQLExecutor sql执行器
	TaskCounter        *sync.WaitGroup           // task counter {one table one task }
	LastCheck          *bpct.CheckPoint          // last check point
	downCounter        *sync.WaitGroup           // down load file counter
	tarCounter         *sync.WaitGroup           // tar -xf file counter
	parseCounter       *sync.WaitGroup           // parse file counter
	check              *bpct.CheckPoint          // current check point
	isEmpty            bool                      // to show whether the files data is empty
	pubDayFiles        inter.FileNames           // public day sorted files
	remoteDayFiles     inter.FileNames           // remoteDayFiles Sorted files
	remoteHourTarFiles inter.FileNames           // remoteHourTarFiles Sorted files
	remoteMinTarFiles  inter.FileNames           // remoteMinTarFiles Sorted files
	remoteSecTarFiles  inter.FileNames           // remoteSecTarFiles Sorted files
	logFileChan        chan *inter.FileName      // chan log file names
	tarFileChan        chan *inter.FileName      // chan tar file names
	downFileChan       chan *inter.FileName      // download file names
	buffer             *bytes.Buffer             // write buffer
	parser             *replication.BinlogParser // binlog parser
	maxPkgSize         int64                     // max package size
	desc               []byte                    // format description event
}

// StartPubRecover recover public directory data
func (r *Recover) StartPubRecover(d *inter.FileName, end int64, c *client.CFSClient, m *db.MetaConf) {
	// clear recover
	defer func() {
		// close channel then wait
		close(r.downFileChan)

		// down counter done then close then need to close download file name
		r.downCounter.Wait()

		// close log file channel then wait
		close(r.logFileChan)

		// wait for parse counter done
		r.parseCounter.Wait()

		// clear all data
		r.Clear()

		r.TaskCounter.Done()
	}()

	r.preparePub(d, end, c, m)

	if len(r.pubDayFiles) == 0 {
		// no public day files exist
		log.Warn("no public day file exists")
		r.isEmpty = true

		r.downCounter.Done()
		r.parseCounter.Done()

		return
	}

	go r.execute()

	go r.down()

	// input down files
	for _, f := range r.pubDayFiles {
		log.Debug("download file ", f)
		r.downFileChan <- f
	}
}

// StartTableRecover d: dir, end: c: client, m : meta config
func (r *Recover) StartTableRecover(d *inter.FileName, end int64, c *client.CFSClient, m *db.MetaConf) {
	defer func() {
		// close down file chan
		close(r.downFileChan)
		// wait
		r.downCounter.Wait()

		// close tar file chan
		close(r.tarFileChan)
		// wait
		r.tarCounter.Wait()

		// close log file chan
		close(r.logFileChan)
		// wait
		r.parseCounter.Wait()

		// clear recover
		r.Clear()

		r.TaskCounter.Done()
	}()

	r.prepareData(d, end, c, m)

	if len(r.remoteDayFiles) == 0 && len(r.remoteHourTarFiles) == 0 && len(r.remoteMinTarFiles) == 0 && len(r.remoteSecTarFiles) == 0 {
		// no remote files to dump from cfs then just return
		log.Warn("no remote files download for table ", r.Table)
		r.isEmpty = true

		r.downCounter.Done()
		r.tarCounter.Done()
		r.parseCounter.Done()

		return
	}

	// start execute log files
	go r.execute()

	// start xtar files
	go r.xTarFiles()

	// download files from remote
	go r.down()

	// input down files
	for _, f := range r.remoteDayFiles {
		r.downFileChan <- f
	}

	for _, f := range r.remoteHourTarFiles {
		r.downFileChan <- f
	}

	for _, f := range r.remoteMinTarFiles {
		r.downFileChan <- f
	}

	for _, f := range r.remoteSecTarFiles {
		r.downFileChan <- f
	}
}

// filterFile  filter single file true: means before check point timestamp, false: means after check point timestamp
func (r *Recover) filterFile(f *inter.FileName) bool {
	if r.LastCheck != nil {
		// check type
		ct := r.LastCheck.FileName.Prefix

		// check start, check end
		cs, _ := r.LastCheck.FileName.GetRange()
		switch f.Prefix {
		case inter.DAY:
			switch ct {
			// already recover on hour, minute, second files
			case inter.HOUR, inter.MINUTE, inter.SECOND:
				return true
			case inter.DAY:
				// still recover on day files
				s, _ := f.GetRange()
				return !(s >= cs)
			}
		case inter.HOUR:
			switch ct {
			case inter.MINUTE, inter.SECOND: // already recover on minute, second files
				return true
			case inter.HOUR:
				s, _ := f.GetRange()
				return !(s >= cs)
			case inter.DAY:
				// recover still on day files
				return false
			}
		case inter.MINUTE:
			switch ct {
			case inter.SECOND: // already recover on second log files
				return true
			case inter.MINUTE: // already recover on minute log files
				// new file names
				s, _ := f.GetRange()
				return !(s >= cs)
			case inter.HOUR, inter.DAY: // still recover on hour and day files
				return false
			}
		case inter.SECOND:
			switch ct {
			case inter.SECOND: // already recover on second log files
				// new file names
				s, _ := f.GetRange()
				return !(s >= cs)
			case inter.MINUTE, inter.HOUR, inter.DAY: // still recover on hour and day files
				return false
			}
		}
	}

	// all is after
	return true
}

// filterFiles  file-filter to filter file before checkpoint
func (r *Recover) filterFiles(fs inter.FileNames, t inter.FileType) inter.FileNames {
	if r.LastCheck != nil {
		// check type
		ct := r.LastCheck.FileName.Prefix

		// check start, check end
		cs, _ := r.LastCheck.FileName.GetRange()
		switch t {
		case inter.DAY:
			// recover on day files
			switch ct {
			case inter.HOUR, inter.MINUTE, inter.SECOND:
				return inter.FileNames{}
			case inter.DAY:
				// new file names
				nfs := inter.FileNames{}
				for _, f := range fs {
					s, _ := f.GetRange()
					if s >= cs {
						nfs = append(nfs, f)
					}
				}
				return nfs
			}
		case inter.HOUR:
			// recover on hour files
			switch ct {
			case inter.MINUTE, inter.SECOND:
				return inter.FileNames{}
			case inter.HOUR:
				// new file names
				nfs := inter.FileNames{}
				for _, f := range fs {
					s, _ := f.GetRange()
					if s >= cs {
						nfs = append(nfs, f)
					}
				}
				return nfs
			case inter.DAY:
				// recover still on day files
				return fs
			}
		case inter.MINUTE:
			switch ct {
			case inter.SECOND: // already recover on second log files
				return inter.FileNames{}
			case inter.MINUTE: // already recover on minute log files
				// new file names
				nfs := inter.FileNames{}
				for _, f := range fs {
					s, _ := f.GetRange()
					if s >= cs {
						nfs = append(nfs, f)
					}
				}
				return nfs
			case inter.HOUR, inter.DAY: // still recover on hour and day files
				return fs
			}
		case inter.SECOND:
			switch ct {
			case inter.SECOND: // already recover on second log files
				// new file names
				nfs := inter.FileNames{}
				for _, f := range fs {
					s, _ := f.GetRange()
					if s >= cs {
						nfs = append(nfs, f)
					}
				}
				return nfs
			case inter.MINUTE, inter.HOUR, inter.DAY: // still recover on hour and day files
				return fs
			}
		}
	}

	return fs
}

// preparePub prepare public
func (r *Recover) preparePub(d *inter.FileName, end int64, c *client.CFSClient, m *db.MetaConf) {
	// find each suitable table dir files
	r.SqlExecutor = m.Copy()
	r.maxPkgSize = r.SqlExecutor.GetMaxAllowedPackage()

	// init  download file counter
	r.downCounter = &sync.WaitGroup{}
	r.downCounter.Add(1)

	// init parse file counter
	r.parseCounter = &sync.WaitGroup{}
	r.parseCounter.Add(1)

	// check
	r.check = &bpct.CheckPoint{
		Table: r.Table,
	}

	// no need to conscious about the max value
	r.pubDayFiles, _ = c.SelectFiles(d, inter.DAY, inter.LogSuffix, 0, r.End)

	// filter day files
	r.pubDayFiles = r.filterFiles(r.pubDayFiles, inter.DAY)

	r.Client = c

	r.buffer = bytes.NewBuffer(nil)

	r.logFileChan = make(chan *inter.FileName, 64)

	r.downFileChan = make(chan *inter.FileName, 64)

	r.parser = replication.NewBinlogParser()

	r.isEmpty = false
}

// prepareData prepareData for download xtar execute stages
func (r *Recover) prepareData(d *inter.FileName, end int64, c *client.CFSClient, m *db.MetaConf) {
	// find each suitable table dir files
	r.SqlExecutor = m.Copy()
	r.maxPkgSize = r.SqlExecutor.GetMaxAllowedPackage()

	// init  download file counter
	r.downCounter = &sync.WaitGroup{}
	r.downCounter.Add(1)

	// init tar file counter
	r.tarCounter = &sync.WaitGroup{}
	r.tarCounter.Add(1)

	// init parse file counter
	r.parseCounter = &sync.WaitGroup{}
	r.parseCounter.Add(1)

	// check
	r.check = &bpct.CheckPoint{
		Table: r.Table,
	}

	// day max
	var dmax = int64(0)
	// find day files
	r.remoteDayFiles, dmax = c.SelectFiles(d, inter.DAY, inter.LogSuffix, 0, end)

	// filter day files
	r.remoteDayFiles = r.filterFiles(r.remoteDayFiles, inter.DAY)

	var hmax = int64(0)
	// find hour files
	r.remoteHourTarFiles, hmax = c.SelectFiles(d, inter.HOUR, inter.TarSuffix, dmax, end)

	var mMax = max(dmax, hmax)
	// find minute files
	r.remoteMinTarFiles, mMax = c.SelectFiles(d, inter.MINUTE, inter.TarSuffix, mMax, end)

	var sMax = max(dmax, hmax, mMax)
	// find second files
	r.remoteSecTarFiles, _ = c.SelectFiles(d, inter.SECOND, inter.TarSuffix, sMax, end)

	r.Client = c

	r.buffer = bytes.NewBuffer(nil)

	r.logFileChan = make(chan *inter.FileName, 64)

	r.tarFileChan = make(chan *inter.FileName, 64)

	r.downFileChan = make(chan *inter.FileName, 64)

	r.parser = replication.NewBinlogParser()

	r.isEmpty = false
}

// xTarFiles 解压文件 execute in local
func (r *Recover) xTarFiles() {
	log.Debug("x tar files")
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}

		r.tarCounter.Done()
	}()

	for {
		// parse each local binlog files
		select {
		case <-r.After.Ctx.Done():
			// done
			return
		case n, hasMore := <-r.tarFileChan:
			if !hasMore {
				log.Warn("all tar file extracted")
				return
			}

			if err := r.x(n); err != nil {
				log.Error("xtar file error ", err)
				panic(err)
				return
			}
		}
	}
}

// x  tar xf 解壓文件
func (r *Recover) x(t *inter.FileName) error {
	f := fmt.Sprintf("%s/%s", t.Path, t.Name)
	log.Debug("tar -xf file name ", f)
	// tar buffer
	tbf, err := os.OpenFile(f, os.O_RDONLY, inter.FileMode)
	if err != nil {
		log.Error(err)
		return err
	}
	defer func() {
		tbf.Close()

		// remove file
		os.Remove(fmt.Sprintf("%s/%s", t.Path, t.Name))
	}()

	// logs file names
	var lsf inter.FileNames

	// tar reader
	tr := tar.NewReader(tbf)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			log.Error(err)
			return err
		}

		// log file name
		logf := &inter.FileName{
			Path:   t.Path,
			Name:   hdr.Name,
			Prefix: t.Prefix,
			Suffix: inter.LogSuffix,
		}

		if s, e := logf.GetRange(); r.filterFile(logf) || e > r.End || s > r.End {
			// need to filtered file or timestamp is exceed
			continue
		}

		if err := copylog(logf, tr); err != nil {
			log.Error(err)
			return err
		}

		lsf = append(lsf, logf)
	}

	// sort  log file names
	sort.Sort(lsf)

	log.Debug("write tar -xf log file into local task")
	for _, l := range lsf {
		r.logFileChan <- l
	}
	return nil
}

// copylog copy log from reader to
func copylog(logf *inter.FileName, tr *tar.Reader) error {
	// create local binlog file
	lf, err := os.OpenFile(fmt.Sprintf("%s/%s", logf.Path, logf.Name), os.O_CREATE|os.O_RDWR|os.O_TRUNC, inter.FileMode)
	if err != nil {
		log.Error(err)
		return err
	}
	defer lf.Close()

	if _, err := io.Copy(lf, tr); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// down rfs； remote files  lfs: local files
func (r *Recover) down() {
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}

		// done file counter
		r.downCounter.Done()
	}()
	for {
		select {
		case <-r.After.Ctx.Done():
			// done
			return
		case df, hasMore := <-r.downFileChan:
			if !hasMore {
				log.Debug("all file downloaded")
				return
			}

			if err := r.trans(df); err != nil {
				log.Error("download data from remote error ", err)
				panic(err)
				return
			}
		}
	}
}

// trans trans file from remote to local
func (r *Recover) trans(rf *inter.FileName) error {
	log.Debug("download file from remote ", rf)
	lf := &inter.FileName{
		Path:   r.TempPath,
		Name:   rf.Name,
		Suffix: rf.Suffix,
	}

	df := fmt.Sprintf("%s%s", inter.StdPath(rf.Path), rf.Name)
	log.Debug("download file name ", df)

	// client read file
	reader, err := r.Client.OpenFile(df)
	if err != nil {
		log.Error(err)
		return err
	}
	// close file
	defer reader.Close()

	// target file
	tf, err := os.OpenFile(fmt.Sprintf("%s%s", lf.Path, lf.Name), os.O_CREATE|os.O_RDWR|os.O_TRUNC, inter.FileMode)
	if err != nil {
		log.Error(err)
		return err
	}
	defer tf.Close()

	if _, err := io.Copy(tf, reader); err != nil {
		log.Error(err)
		return err
	}

	switch rf.Suffix {
	case inter.LogSuffix:
		r.logFileChan <- lf
	case inter.TarSuffix:
		r.tarFileChan <- lf
	}

	return nil
}

// execute execute binlog statement
func (r *Recover) execute() {
	log.Debug("execute")
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}

		// done parse file counter
		r.parseCounter.Done()
	}()
	onEventFunc := func(e *replication.BinlogEvent) error {
		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			// sql executor begin
			if err := r.SqlExecutor.Begin(); err != nil {
				log.Error(err)
				return err
			}

			r.desc = utils.Base64Encode(e.RawData)
			// sql executor
			if err := r.SqlExecutor.Execute([]byte(fmt.Sprintf("BINLOG '\n%s\n'%s", r.desc, inter.Delimiter))); err != nil {
				log.Error(err)
				return err
			}

			// sql executor commit
			if err := r.SqlExecutor.Commit(); err != nil {
				log.Error(err)
				return err
			}
		case replication.QUERY_EVENT:
			qe := e.Event.(*replication.QueryEvent)
			switch strings.ToUpper(string(qe.Query)) {
			case "BEGIN":
				if err := r.SqlExecutor.Begin(); err != nil {
					log.Error(err)
					return err
				}
			case "COMMIT":
				if err := r.SqlExecutor.Commit(); err != nil {
					log.Error(err)
					return err
				}
			case "ROLLBACK":
			case "SAVEPOINT":
			default:
				if err := r.SqlExecutor.Begin(); err != nil {
					log.Error(err)
					return err
				}

				// first use db
				if e.Header.Flags&inter.LogEventSuppressUseF == 0 && qe.Schema != nil && len(qe.Schema) != 0 {
					use := fmt.Sprintf("use %s", qe.Schema)
					if err := r.SqlExecutor.Execute([]byte(use)); err != nil {
						log.Error(err)
						return err
					}
				}

				// then execute ddl
				if err := r.SqlExecutor.Execute(qe.Query); err != nil {
					log.Error(err)
					return err
				}
				if err := r.SqlExecutor.Commit(); err != nil {
					log.Error(err)
					return err
				}
			}
		case replication.XID_EVENT:
			if err := r.SqlExecutor.Commit(); err != nil {
				log.Error(err)
				return err
			}
		case replication.TABLE_MAP_EVENT:
			// write to buffer first
			r.buffer.WriteString(fmt.Sprintf("BINLOG '\n%s", utils.Base64Encode(e.RawData)))
		case replication.WRITE_ROWS_EVENTv0,
			replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv0,
			replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv0,
			replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			r.buffer.WriteString(fmt.Sprintf("\n%s", utils.Base64Encode(e.RawData)))

			if e.Event.(*replication.RowsEvent).Flags == blog.StmtEndFlag {
				r.buffer.WriteString(fmt.Sprintf("\n'%s", inter.Delimiter))

				bs := r.buffer.Bytes()
				l := len(bs)

				if int64(l) > r.maxPkgSize {
					if err := r.resetMaxPkgSize(int64(l) << 1); err != nil {
						log.Error(err)
						return err
					}
				}

				// execute
				if err := r.SqlExecutor.Execute(bs); err != nil {
					log.Error(err)
					return err
				}

				// reset buffer for reuse
				r.buffer.Reset()
			}
		case replication.ROTATE_EVENT:
			if e.Header.LogPos != 0 {
				//if err := r.SqlExecutor.Execute([]byte("DELIMITER ;")); err != nil {
				//	log.Error(err)
				//	return err
				//}
			}
		}
		// save offset
		r.check.Offset = int64(e.Header.LogPos)
		return nil
	}

	for {
		// parse each local binlog files
		select {
		case <-r.After.Ctx.Done():
			return
		case n, hasMore := <-r.logFileChan:
			if !hasMore {
				log.Warn("all file parsed")
				return
			}

			log.Debug("file name ", n)
			pos := int64(4)
			r.check.FileName, r.check.Offset = n, pos

			if r.LastCheck != nil && strings.EqualFold(r.LastCheck.FileName.Name, n.Name) {
				// start from the last break offset
				pos = r.LastCheck.Offset
			}

			pf := fmt.Sprintf("%s/%s", n.Path, n.Name)

			if err := r.parser.ParseFile(pf, pos, onEventFunc); err != nil {
				// write check to io writer
				log.Error("parse file error ", pf, ", error ", err)
				panic(err)
				return
			}
		}
	}
}

// setDelimiter 設置delimiter
func (r *Recover) setDelimiter() error {
	if err := r.SqlExecutor.Execute([]byte(fmt.Sprintf("DELIMITER %s\nROLLBACK %s", inter.Delimiter, inter.Delimiter))); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// Clear clear all statement
func (r *Recover) Clear() {
	log.Debug("clear ")
	r.SqlExecutor.Close()

	if !r.isEmpty {
		r.check.Write(r.Writer)
	}

	r.parser.Stop()

	r.buffer.Reset()
}

func (r *Recover) resetMaxPkgSize(s int64) error {
	r.maxPkgSize = s
	set := fmt.Sprintf("set global max_allowed_packet=%d", r.maxPkgSize)
	// execute
	if err := r.SqlExecutor.Execute([]byte(set)); err != nil {
		log.Error(err)
		return err
	}

	// reconnect MySQL
	r.SqlExecutor.RefreshConnection()

	// sql executor begin
	if err := r.SqlExecutor.Begin(); err != nil {
		log.Error(err)
		return err
	}

	// sql executor
	if err := r.SqlExecutor.Execute([]byte(fmt.Sprintf("BINLOG '\n%s\n'%s", r.desc, inter.Delimiter))); err != nil {
		log.Error(err)
		return err
	}

	// sql executor commit
	if err := r.SqlExecutor.Commit(); err != nil {
		log.Error(err)
		return err
	}

	// sql executor begin
	if err := r.SqlExecutor.Begin(); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// max max value
func max(a ...int64) int64 {
	m := int64(0)
	for _, i := range a {
		if i > m {
			m = i
		}
	}

	return m
}
