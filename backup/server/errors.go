package server

const (
	Success          = 1000
	AlreadyDumpError = 1100
	ReadRequestError = 1101
	DumpError        = 1102
	ClusterIDNotFoundError = 1103
)

var (
	Errs = map[int64]string{
		Success:          "success",
		AlreadyDumpError: "already on dumping status for cluster id{%d}",
		ReadRequestError: "read request error %v",
		DumpError:        "dump cluster id{%d} error{%v}",
		ClusterIDNotFoundError: "cluster id {%d} not found on this server",
	}
)
