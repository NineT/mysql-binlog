package meta

// Offset binlog offset write to meta
type Offset struct {
	ClusterID  int64  `json:"clusterid"` // cluster id
	MergedGtid []byte `json:"gtid"`      // gtid
	Time       uint32 `json:"time"`      // timestamp
	BinFile    string `json:"file"`      // binlog File
	BinPos     uint32 `json:"pos"`       // binlog position
	Counter    int                       // counter
	Header     bool                      // header flag
	OriGtid    []byte                    // origin gtid means current gtid event
}

// Meta data interface
type IMeta interface {
	// Read meta offset from meta storage
	Read(k interface{}) (*Offset, error)

	// Save node to storage
	Save(offset *Offset) error
}