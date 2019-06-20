package client

import (
	"archive/tar"
	"bufio"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/mysql-binlog/common/db"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
)

const (
	// BinlogOffset file name
	BinlogIndexFile = "bin.index"
)

// CFSClient  cfs客户端
type CFSClient struct {
	Path     string         // 存储路径根目录
	Compress bool           // 是否开启压缩
	DbReg    *regexp.Regexp // 库正则表达式
	TbReg    *regexp.Regexp // 表正则表达式
}

// CFSFile  cfs文件
type CFSFile struct {
	FileName string    // FileName  文件名称
	compress bool      // compress 是否压缩
	f        *os.File  // f file
	W        io.Writer // Writer    fileWrite
	R        io.Reader // Reader    file reader
}

// CreateFile  创建新CFS客户端 仅仅传入文件名
func (s *CFSClient) CreateFile(path, file string) (inter.IFile, error) {
	path = s.Path + path
	log.Debug("path ", path, ", file name ", file)
	// create dir
	if _, err := os.Stat(s.Path + path); os.IsNotExist(err) {
		os.MkdirAll(path, os.ModePerm)
	}

	c := &CFSFile{
		FileName: file,
		compress: s.Compress,
	}

	// 完整路径文件
	f, err := os.OpenFile(fmt.Sprintf("%s/%s", path, file), os.O_CREATE|os.O_RDWR|os.O_TRUNC, inter.FileMode)
	if err != nil {
		final.Terminate(err, nil)
	}

	// set file
	c.f = f

	c.W = f
	if s.Compress { // 开启压缩
		// zlib 压缩数据
		iw := zlib.NewWriter(f)

		// set writer
		c.W = iw
	}

	return c, nil
}

/**
打包的文件总是以打包的 日志文件前缀为标志
day => 不需要合并
hour => 总是合并当前1天hour的数据
minute => 总是合并当天的minute 数据
second => 总是合并当天second数据

最終合併的結果： 只生成四個文件
*/

// CTarFiles  压缩文件最大程度保留大时间间隔文件 保证恢复数据速度
func (s *CFSClient) CTarFiles(path string, fileType inter.FileType, start, end int64) error {
	switch fileType {
	case inter.DAY:
		// day 文件不参与合并
		return nil
	}
	ap := inter.AbsolutePath{
		FileType: fileType,
		Start:    start,
		End:      end,
	}

	path = s.Path + path

	// files 至少有一個文件
	m, fs := s.getRangeFiles(path, fileType, start, end)

	// tar file name 如果存在满足条件的 压缩包 则无需创建新的压缩包
	tf := s.getNearestTarFile(path, m, fileType)
	if tf == nil {
		log.Debug("no suitable tar file exist on cfs")
		fileName := fmt.Sprintf("%s/%s", path, ap.DstTarFileName())
		// tar file name
		log.Debug("file type ", fileType, ", tar file name ", fileName)

		// create tar file
		zf, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, inter.FileMode)
		if err != nil {
			log.Fatal(err)
			return errors.Trace(err)
		}
		defer zf.Close()

		// create tar write
		tw := tar.NewWriter(zf)
		defer tw.Close()

		// 打包文件
		if err := s.packFiles(tw, fs); err != nil {
			return err
		}
	} else {
		log.Debug("find suitable tar file then reopen the right tar file ", tf.Name)
		st, _ := tf.GetRange()

		// 新文件需要新的 起始时间戳
		nap := &inter.AbsolutePath{
			FileType: fileType,
			Start:    st,
			End:      end,
		}

		// 重命名文件
		src := fmt.Sprintf("%s/%s", tf.Path, tf.Name)
		dst := fmt.Sprintf("%s/%s", tf.Path, nap.DstTarFileName())
		log.Debug("rename file from ", src, " to ", dst)
		if err := os.Rename(src, dst); err != nil {
			log.Fatal(err)
			return err
		}

		// open tar file
		zf, err := os.OpenFile(dst, os.O_RDWR, inter.FileMode)
		if err != nil {
			log.Fatal(err)
			return errors.Trace(err)
		}
		defer zf.Close()

		if _, err := zf.Seek(-2<<9, os.SEEK_END); err != nil {
			log.Fatal(err)
			return err
		}

		// create zip write
		tw := tar.NewWriter(zf)
		defer tw.Close()

		// 打包文件
		if err := s.packFiles(tw, fs); err != nil {
			return err
		}
	}

	s.RemoveFiles(fs)

	return nil
}

func (s *CFSClient) XTarFiles(tf string) ([]inter.IFile, error) {
	return nil, nil
}

func (c *CFSFile) Write(b []byte) (n int, err error) {
	return c.W.Write(b)
}

func (c *CFSFile) Read(p []byte) (n int, err error) {
	return c.R.Read(p)
}

// Close 关闭文件结构
func (c *CFSFile) Close() error {
	if c.compress && c.W != nil {
		c.W.(*zlib.Writer).Flush()
	}

	c.R = nil
	c.W = nil
	return c.f.Close()
}

// getRangeFiles 获取区间范围文件
func (s *CFSClient) getRangeFiles(path string, fileType inter.FileType, start, end int64) (int64, inter.FileNames) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		final.Terminate(err, nil)
	}

	m := start
	ls := inter.FileNames{}
	for _, f := range files {
		fn := f.Name()
		if strings.HasPrefix(fn, string(fileType)) && strings.HasSuffix(fn, string(inter.LogSuffix)) {
			// 获取天有效文件
			log.Debug("valid file name ", fn)
			l := &inter.FileName{
				Name:   fn,
				Path:   path,
				Suffix: inter.LogSuffix,
			}

			s, e := l.GetRange()
			if e <= end {
				ls = append(ls, l)
			}

			if s <= m {
				// 获取最小值 区间
				m = s
			}
		}
	}

	return m, ls
}

// RemoveFiles 刪除文件
func (s *CFSClient) RemoveFiles(names inter.FileNames) {
	for _, n := range names {
		os.Remove(fmt.Sprintf("%s/%s", n.Path, n.Name))
	}
}

// getNearestTarFile 获取最近的tar文件
func (s *CFSClient) getNearestTarFile(path string, m int64, fileType inter.FileType) *inter.FileName {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		final.Terminate(err, nil)
	}

	for _, f := range files {
		fn := f.Name()
		if strings.HasPrefix(fn, string(fileType)) && strings.HasSuffix(fn, string(inter.TarSuffix)) {
			// 获取天有效文件
			l := &inter.FileName{
				Name:   fn,
				Path:   path,
				Suffix: inter.TarSuffix,
			}

			s, e := l.GetRange()

			if e-s < int64(inter.DaySeconds) && e <= m {
				// 如果时间超过1天 并且 end值保持连续
				return l
			}
		}
	}

	return nil
}

func (s *CFSClient) packFiles(tw *tar.Writer, fs inter.FileNames) error {
	for _, n := range fs {
		fn := fmt.Sprintf("%s/%s", n.Path, n.Name)
		log.Debug("open file ", fn)

		// read file
		rf, err := os.OpenFile(fn, os.O_RDONLY, inter.FileMode)
		if err != nil {
			log.Fatal(err)
			return err
		}

		// get file info
		fi, _ := rf.Stat()

		log.Debug("file header ", n.Name)

		// write tar header
		if err := tw.WriteHeader(&tar.Header{
			Name: n.Name,
			Mode: 0600,
			Size: fi.Size(),
		}); err != nil {
			log.Fatal(err)
			return err
		}

		if _, err := io.Copy(tw, rf); err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

func (s *CFSClient) OpenFile(filename string) (inter.IFile, error) {
	// open tar file
	file, err := os.OpenFile(filename, os.O_RDONLY, inter.FileMode)
	if err != nil {
		log.Fatal(err)
		return nil, errors.Trace(err)
	}

	cf := &CFSFile{
		FileName: filename,
		compress: s.Compress,
		f:        file,
		R:        file, // default: using file as reader
	}

	if s.Compress { // 开启压缩
		// zlib 压缩数据
		r, err := zlib.NewReader(file)
		if err != nil {
			log.Fatal("error : file not in compress type while parameter compress set to true")
			final.Terminate(err, nil)
		}

		cf.R = r
	}

	return cf, nil
}

// SelectTableDirs 选择满足要求表文件路径
func (s *CFSClient) SelectTableDirs() inter.FileNames {
	log.Debug("select table dirs ")
	ns, err := ioutil.ReadDir(s.Path)
	if err != nil {
		final.Terminate(err, nil)
	}

	// matched files
	var mf inter.FileNames

	for _, n := range ns {
		part := strings.Split(n.Name(), ".")

		if len(part) < 2 {
			// part
			continue
		}

		db := part[0]
		tb := part[1]

		if s.DbReg.Match([]byte(db)) && s.TbReg.Match([]byte(tb)) {
			mf = append(mf, &inter.FileName{
				Path: s.Path,
				Name: n.Name(),
			})
		}
	}

	return mf
}

// SelectFiles 选择满足时间范围的文件名
func (s *CFSClient) SelectFiles(dir *inter.FileName, pre inter.FileType, suf inter.FileSuffix, start, end int64) (inter.FileNames, int64) {
	p := fmt.Sprintf("%s%s", inter.StdPath(dir.Path), dir.Name)
	log.Debug("table path ", p)
	ns, err := ioutil.ReadDir(p)
	if err != nil {
		final.Terminate(err, nil)
	}

	// max timestamp
	max := int64(0)
	// file names
	fns := inter.FileNames{}
	for _, n := range ns {
		name := n.Name()
		if !strings.HasPrefix(name, string(pre)) {
			continue
		}

		f := &inter.FileName{
			Path:   p,
			Name:   name,
			Suffix: suf,
			Prefix: pre,
		}

		s, e := f.GetRange()

		switch suf {
		case inter.LogSuffix:
			// day files 必须包含在区间以内
			if s >= start && e <= end {
				fns = append(fns, f)
				if e >= max {
					max = e
				}
			}
		case inter.TarSuffix:
			// 只要判断区间有交集 则append
			if (start >= s && start <= e) || (end >= s && end <= e) {
				fns = append(fns, f)
				if e <= end && e >= max {
					// 包含区间内的最大值
					max = e
				}
			}
		}
	}

	if len(fns) >= 0 {
		sort.Sort(fns)
	}

	return fns, max
}

func (s *CFSClient) PublicPath() *inter.FileName {
	return &inter.FileName{
		Path: s.Path,
		Name: inter.Public,
	}
}

// ReadLastOffset
func (s *CFSClient) ReadLastOffset() (*db.BinlogOffset, error) {
	f, err := os.Open(s.Path + BinlogIndexFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	last := ""
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

		if err != nil || io.EOF == err {
			if line == "" {
				break
			}
		}
		last = line
	}

	if last == "" {
		// last is empty so no error
		log.Warn("no offset exist on cfs directory")
		return nil, nil
	}

	off := &db.BinlogOffset{}
	if err := json.Unmarshal([]byte(last), off); err != nil {
		log.Warn("data format no right , ", last)
		return nil, err
	}

	return off, nil
}
