package res

import (
	"github.com/mysql-binlog/common/inter"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"

	"github.com/zssky/log"
)

// rangeLogs using start, end
func rangeLogs(start, end int64, p string) ([]int64, error) {
	// find all binlog file between start and end
	fs, err := ioutil.ReadDir(p)
	if err != nil {
		log.Errorf("read dir %s error {%v}", p, err)
		return nil, err
	}

	// result
	var rst inter.Int64s
	for _, f := range fs {
		n := f.Name()
		if strings.HasSuffix(n, logSuffix) {
			// totally log files

			ts := strings.TrimSuffix(n, logSuffix)
			t, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				log.Errorf("parse int value{%s} failed {%v}", ts, err)
				return nil, err
			}

			if t >= start && t <= end {
				rst = append(rst, t)
			}
		}
	}

	if len(rst) == 0 {
		log.Warnf("no suitable log file found on path{%s} between{%d, %d}", p, start, end)
		return []int64{}, nil
	}

	sort.Sort(rst)

	return rst, nil
}

// latestTime find the latestTime log file
func latestTime(i int64, path string) (int64, error) {
	fs, err := ioutil.ReadDir(path)
	if err != nil {
		log.Errorf("read dir %s error {%v}", path, err)
		return 0, err
	}

	mx := i
	// range files
	for _, f := range fs {
		n := f.Name()
		if strings.HasSuffix(n, logSuffix) {
			ts := strings.TrimSuffix(n, logSuffix)
			t, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				log.Errorf("parse int{%s} error {%v}", ts, err)
				return 0, err
			}

			if t > i {
				// continue for timestamp is bigger than
				continue
			}
			// using max timestamp distance
			dist := i - t
			if dist > 0 && mx > dist {
				mx = dist
			}
		}
	}

	// error for max value not have changed then means no snapshot get error
	if mx == i {
		log.Warnf("no increment data under{%s} before snapshot {snapshot_%d}", path, i)
		return i, nil
	}

	// return absolute file path with no error
	return i - mx, nil
}
