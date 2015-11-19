package dataio
import "strconv"

func New() *dataio {
	return &dataio{
		Ddbio: NewDB(),
		cio: NewCache(),
	}
}

type dataio struct {
	Ddbio 	*ddbio
	cio   	*cio
}

// -------------------------------------------------
// 디비와 캐쉬를 동시에 사용
// -------------------------------------------------
// 1. 캐쉬에서 읽고 키가 없으면,
// 2. 디비에서 읽음.
// 3. 디비에서 읽었으면, 캐쉬에 저장.
func (io *dataio)Read2Way(hkey string, hid string, hkey2 string, hid2 string) (map[string]interface{}, error) {
	// 1. 캐쉬에서 읽고 키가 없으면,
	resp, err := io.cio.readHashItem(hkey, hid, hkey2, hid2)
	if err != nil {
		return resp, err
	}
	// 값이 캐쉬에 이미 존재하면 바로 리턴함.
	if resp != nil {
		return resp, err
	}

	// 2. 캐쉬에 없으므로, 디비에서 읽음.
	resp, err = io.Ddbio.readHashItem(hkey, hid, hkey2, hid2)
	if err != nil {
		return resp, err
	}

	// 3. 디비에서 읽었으면, 캐쉬에 저장.
	err = io.cio.writeHashItem(hkey, hid, hkey2, hid2, resp)
	if err != nil {
		return nil, err
	}

	return resp, err
}

// 1. 디비 / 캐쉬에 쓰기
func (io *dataio)Write2Way(hkey string, hid string, hkey2 string, hid2 string, updateAttrs map[string]interface{}) (error) {
	err := io.Ddbio.writeHashItem(hkey, hid, hkey2, hid2, updateAttrs)
	if err != nil {
		return err
	}
	err = io.cio.writeHashItem(hkey, hid, hkey2, hid2, updateAttrs)
	if err != nil {
		return err
	}

	return nil
}

func (io *dataio)Del2Way(hkey string, hid string, hkey2 string, hid2 string) (error) {
	err := io.Ddbio.delHashItem(hkey, hid, hkey2, hid2)
	if err != nil {
		return err
	}
	err = io.cio.delHashItem(hkey, hid, hkey2, hid2)
	if err != nil {
		return err
	}

	return nil
}

/*
// -------------------------------------------------
// 기타 API 들
// -------------------------------------------------
// Dynamo DB 관련
func (io *dataio)CreateHashTable(tableName string, readCap int, writeCap int) error {
	if err := io.Ddbio.createHashTable(tableName, readCap, writeCap); err != nil {
		return err
	}
	io.Ddbio.waitUntilStatus(tableName, "ACTIVE")
	return nil
}

func (io *dataio)ListTables() ([]*string, error) {
	ret, err := io.Ddbio.listTables()
	return ret.TableNames, err
}

func (io *dataio)DeleteTable(tableName string) error {
	list, err := io.ListTables()
	if err != nil {
		return err
	}
	if io.Ddbio.isExistTableByName(list, tableName) {
		if err := io.Ddbio.deleteTable(tableName) ; err != nil {
			return err
		}
	}

	return err
}

// Cache 관련
func (io *dataio)CacheFlushDB() error {
	return io.cio.FlushDB()
}

func (io *dataio)CacheGetTTL() int {
	return io.cio.GetTTL()
}

func (io *dataio)CacheSetTTL(sec int) {
	io.cio.SetTTL(sec)
}
*/

// -------------------------------------------------
// user : taskbytime interface
// -------------------------------------------------
func (io *dataio)ReadUserTask(uid int, tid int) (map[string]interface{}, error) {
	resp, err := io.Read2Way(KEY_USER, strconv.Itoa(uid), KEY_TASK, strconv.Itoa(tid))
	return resp, err
}

func (io *dataio)WriteUserTask(uid int, tid int, updateAttrs map[string]interface{}) (error) {
	err := io.Write2Way(KEY_USER, strconv.Itoa(uid), KEY_TASK, strconv.Itoa(tid), updateAttrs)
	return err
}

func (io *dataio)DelUserTask(uid int, tid int) (error) {
	err := io.Del2Way(KEY_USER, strconv.Itoa(uid), KEY_TASK, strconv.Itoa(tid))
	return err
}
