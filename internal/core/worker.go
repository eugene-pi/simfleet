package core

// Batch is one poll's worth of records.
type Batch struct {
	Records []Record
	Closed  bool // client shut down / клиент закрыт
}

type Record struct {
	Partition int32
	Offset    int64
	Value     []byte
}
