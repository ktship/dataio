package dataio

func New() dataio {
	return dataio{
		ddbio: NewDB(),
		cio: NewCache(),
	}
}

type dataio struct {
	ddbio 	*ddbio
	cio   	*cio
}

// Read : First Cache, Last DB
// Cache 에서 먼저 읽고 키가 없으면 DB 에서 읽음.
// DB 에서 읽었으면 Cache 에 저장해둠.
//func (dio *dataio)ReadFCLD()