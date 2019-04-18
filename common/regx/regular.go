package regx

import (
	"bytes"
	"regexp"
)

var (
	/**
			"CREATE TABLE `position` (`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',",
	"CREATE TABLE `mydb.position`(`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',",
	"CREATE TABLE `mydb`.`position`(`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',",
		"CREATE TABLE `position`(`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',",
		"CREATE				 TABLE `mydb.mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE `mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS `mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS mytable (`id` int(10)) ENGINE=InnoDB",
	 */
	 // CREATE INDEX k_1 ON sbtest1(k)
	expCreateIndex = regexp.MustCompile("(?i)^CREATE\\s+INDEX\\s+(.*?)\\s+ON\\s+`{0,1}([a-z0-9_]*?)`{0,1}\\.{0,1}`{0,1}([a-z0-9_]*?)`{0,1}[\\s\\(]+.*")
	expCreateTable = regexp.MustCompile("(?i)^CREATE\\s+TABLE(\\s+IF\\s+NOT\\s+EXISTS)?\\s+`{0,1}(.*?)[`\\s\\(]+.*")
	expAlterTable  = regexp.MustCompile("(?i)^ALTER\\s+TABLE\\s+.*?`{0,1}([a-z0-9_]*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+.*")
	expRenameTable = regexp.MustCompile("(?i)^RENAME\\s+TABLE\\s+(.*?)[\\s`]+TO[\\s`]+(.*?)$")
	expDropTable   = regexp.MustCompile("(?i)^DROP\\s+TABLE(\\s+IF\\s+EXISTS){0,1}\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}($|\\s)")
)

// DdlType ddl 类型
type DdlType int8

const (
	DdlCreateType      DdlType = 0x0
	DdlAlterType       DdlType = 0x1
	DdlRenameType      DdlType = 0x2
	DdlDropType        DdlType = 0x3
	DdlCreateIndexType DdlType = 0x4
)

// RegMatch match ddl return full table name and is reg matched
func RegMatch(db, ddl []byte) ([][]byte, bool) {
	var mb [][]byte // mb 对应的内存地址还是 ddl内存地址 这里需要注意

	var tables [][]byte

	regexps := []regexp.Regexp{*expCreateTable, *expAlterTable, *expRenameTable, *expDropTable, *expCreateIndex}

	ddlType := DdlType(0)
	for i, reg := range regexps {
		mb = reg.FindSubmatch(ddl)
		if len(mb) != 0 {
			ddlType = DdlType(i)
			break
		}
	}
	mbLen := len(mb)
	if mbLen == 0 {
		return tables, false
	}

	switch ddlType {
	case DdlRenameType:
		rns := bytes.Split(ddl, []byte(","))
		switch len(rns) {
		case 0:
			return tables, false
		case 1:
			tables = append(tables, StripQuoteAndAppendDb(mb[1], db))
			tables = append(tables, StripQuoteAndAppendDb(mb[2], db))

			return tables, true
		default:
			mb := expRenameTable.FindSubmatch(rns[0])

			// add origin table
			tables = append(tables, StripQuoteAndAppendDb(mb[1], db))

			// add new table
			tables = append(tables, StripQuoteAndAppendDb(mb[2], db))

			header := []byte("RENAME TABLE ")
			for i := 1; i < len(rns); i++ {
				var rtl [][]byte

				rtl = append(rtl, header)
				rtl = append(rtl, rns[i])

				mb = expRenameTable.FindSubmatch(bytes.Join(rtl, []byte("")))

				// add origin table
				tables = append(tables, StripQuoteAndAppendDb(mb[1], db))

				// add new table
				tables = append(tables, StripQuoteAndAppendDb(mb[2], db))
			}

			return tables, true
		}
	// using separate reg match
	//expAlterTable  = regexp.MustCompile("(?i)^ALTER\\sTABLE\\s+(`([a-z0-9_]*?)\\.([a-z0-9_]*?)`|`([a-z0-9_]*?)`\\.`([a-z0-9_]*?)`|([a-z0-9_]*?)`([a-z0-9_]*?)`|([a-z0-9_]*?)\\.([a-z0-9_]*?)|([a-z0-9_]*?)([a-z0-9_]*?))\\s+(.*?)$")
	//case DdlAlterType:
	//	i := 2
	//	for ; i < len(mb); i++ {
	//		if mb[i] != nil {
	//			break
	//		}
	//	}
	//
	//	if len(mb[i]) != 0 {
	//		db = mb[i]
	//	}
	//
	//	table := mb[i + 1]
	//
	//	tables = append(tables, StripQuoteAndAppendDb(table, db))
	//	return tables, true
	case DdlAlterType:
		if len(mb[mbLen-2]) != 0 {
			db = mb[mbLen-2]
		}

		tables = append(tables, StripQuoteAndAppendDb(mb[mbLen-1], db))
		return tables, true
	case DdlCreateType:
		tables = append(tables, StripQuoteAndAppendDb(mb[mbLen-1], db))
		return tables, true
	case DdlDropType:
		if len(mb[mbLen-3]) != 0 {
			db = mb[mbLen-3]
		}

		tables = append(tables, StripQuoteAndAppendDb(mb[mbLen-2], db))
		return tables, true
	case DdlCreateIndexType:
		if len(mb[mbLen-2]) != 0 {
			db = mb[mbLen - 2]
		}
	}

	return tables, false
}

// StripQuoteAndAppendDb strip quote and append db
func StripQuoteAndAppendDb(srcTable, srcDb []byte) []byte {
	// reallocate memory address

	table := make([]byte, len(srcTable))
	copy(table, srcTable)

	db := make([]byte, len(srcDb))
	copy(db, srcDb)

	hasDbPrefix := false

	count := 0 // 执行符 数量总和
	size := 0
	for i, bt := range table {
		size++
		switch bt {
		case byte('`'):
			count++
			continue
		case byte('.'):
			hasDbPrefix = true
		}

		table[i-count] = bt
	}

	if !hasDbPrefix { // 没有库名前缀
		fTb := bytes.NewBuffer(db)
		fTb.WriteByte('.')
		fTb.Write(table[0 : size-count])
		return fTb.Bytes()
	}

	return table[0 : size-count]
}
