package regx

import (
	"bytes"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"github.com/zssky/log"
	"strings"
)

// Parse parse ddl and get table
func Parse(ddl []byte, db []byte) ([][]byte, bool) {
	log.Debug("parse ddl ", string(ddl))
	var tables [][]byte

	if strings.HasPrefix(string(ddl), sqlparser.RenameStr) && strings.Contains(string(ddl), ",") {
		// rename sql ddl
		for i, s := range strings.Split(string(ddl), ",") {
			switch i {
			case 0:
			default:
				s = fmt.Sprintf("rename table %s", s)
			}
			ns, err := sqlparser.Parse(s)
			if err != nil {
				log.Error(err)
				// error
				return tables, false
			}

			// partly statement
			switch pst := ns.(type) {
			case *sqlparser.DDL:
				// before table name
				btb := getFullTable(pst.Table.Name.String(), pst.Table.Qualifier.String(), db)
				tables = append(tables, btb)

				// after table name
				atb := getFullTable(pst.NewName.Name.String(), pst.NewName.Qualifier.String(), db)
				tables = append(tables, atb)
			}
		}
		return tables, true
	}

	stmt, err := sqlparser.Parse(string(ddl))
	if err != nil {
		log.Error(err)
		return tables, false
	}

	switch st := stmt.(type) {
	case *sqlparser.DDL:
		switch st.Action {
		case sqlparser.CreateStr:
			fullTb := getFullTable(st.NewName.Name.String(), st.NewName.Qualifier.String(), db)
			tables = append(tables, fullTb)
		case sqlparser.AlterStr, sqlparser.DropStr, sqlparser.TruncateStr, sqlparser.CreateVindexStr, sqlparser.AddColVindexStr, sqlparser.DropColVindexStr:
			fullTb := getFullTable(st.Table.Name.String(), st.Table.Qualifier.String(), db)
			tables = append(tables, fullTb)
		case sqlparser.RenameStr:
			btb := getFullTable(st.Table.Name.String(), st.Table.Qualifier.String(), db)
			tables = append(tables, btb)

			// after table name
			atb := getFullTable(st.NewName.Name.String(), st.NewName.Qualifier.String(), db)
			tables = append(tables, atb)
		}
	case *sqlparser.DBDDL:
		fmt.Println(st.DBName)
	}
	return tables, true
}

// getFullTable tb: table, pdb: parsed db, sdb: send in db
func getFullTable(tb, pdb string, sdb []byte) []byte {
	// table bytes
	tbs := trimSpace(tb)
	if strings.Contains(tb, ".") {
		return tbs
	}

	t := bytes.NewBuffer(nil)

	db := trimSpace(pdb)
	if len(db) == 0 {
		t.Write(sdb)
	} else {
		t.Write(db)
	}
	t.WriteByte(byte('.'))
	t.Write(tbs)
	return t.Bytes()
}

// trimSpace trim space
func trimSpace(s string) []byte {
	b := bytes.NewBuffer(nil)
	for _, c := range s {
		switch c {
		case ' ', '\r', '\n', '\t':
		default:
			b.WriteByte(byte(c))
		}
	}

	return b.Bytes()
}
