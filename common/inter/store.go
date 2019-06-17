package inter

import "io"

/***
存储 适用于存储的不同选择 可以根据客户化定制 但是需要实现接口
*/

// IFile 存储接口
type IFile interface {
	//WriteEvent(p []byte) (n int, err error) // 写数据
	//Close() error                      // 关闭写入
	io.Writer
	io.Reader
	io.Closer
}

// IStorageClient 存储客户端接口
type IClient interface {
	CreateFile(path, fileName string) (IFile, error)                  // CreateFile 创建文件 创建文件
	OpenFile(f string) (IFile, error)                                  // CreateFile
	CTarFiles(path string, fileType FileType, start, end int64) error // CTarFiles 打包文件 tar -cf
	XTarFiles(tf string) ([]IFile, error)                             // XTarFiles 解压文件 tar -xf
}
