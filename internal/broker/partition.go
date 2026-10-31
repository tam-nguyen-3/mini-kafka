package broker

type Partition struct {
	ID  int
	Log *Log
}

func NewPartition(id int) *Partition {
	return &Partition{
		ID:  id,
		Log: NewLog(),
	}
}

func (p *Partition) Append(key, value string) Record {
	return p.Log.Append(key, value)
}

func (p *Partition) ReadFrom(offset int64, maxRecords int) ([]Record, error) {
	return p.Log.ReadFrom(offset, maxRecords)
}
