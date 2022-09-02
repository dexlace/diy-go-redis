package resp

type Connection interface {
	Write([]byte) error
	// GetDBIndex 得到db的索引  redis有16个db
	GetDBIndex() int
	// SelectDB 选择数据库
	SelectDB(int)
}
