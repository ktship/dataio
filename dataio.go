package dataio
import "strconv"

func New() *dataio {
	return &dataio{
		ddbio: NewDB(),
		cio: NewCache(),
	}
}

type dataio struct {
	ddbio 	*ddbio
	cio   	*cio
}

// -------------------------------------------------
// user : task
// -------------------------------------------------
// 1. 캐쉬에서 읽고 키가 없으면,
// 2. 디비에서 읽음.
// 3. 디비에서 읽었으면, 캐쉬에 저장.
func (io *dataio)read2Way(hkey string, hid string, hkey2 string, hid2 string) (map[string]interface{}, error) {
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
	resp, err = io.ddbio.readHashItem(hkey, hid, hkey2, hid2)
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

// 1. 디비에 쓰기
// 2. 캐쉬에 쓰기
func (io *dataio)write2Way(hkey string, hid string, hkey2 string, hid2 string, updateAttrs map[string]interface{}) (error) {
	err := io.ddbio.writeHashItem(hkey, hid, hkey2, hid2, updateAttrs)
	if err != nil {
		return err
	}
	err = io.cio.writeHashItem(hkey, hid, hkey2, hid2, updateAttrs)
	if err != nil {
		return err
	}

	return nil
}

func (io *dataio)del2Way(hkey string, hid string, hkey2 string, hid2 string) (error) {
	err := io.ddbio.delHashItem(hkey, hid, hkey2, hid2)
	if err != nil {
		return err
	}
	err = io.cio.delHashItem(hkey, hid, hkey2, hid2)
	if err != nil {
		return err
	}

	return nil
}

// -------------------------------------------------
// user : taskbytime interface
// -------------------------------------------------
func (io *dataio)ReadUserTask(uid int, tid int) (map[string]interface{}, error) {
	resp, err := io.read2Way(KEY_USER, strconv.Itoa(uid), KEY_TASK, strconv.Itoa(tid))
	return resp, err
}

func (io *dataio)WriteUserTask(uid int, tid int, updateAttrs map[string]interface{}) (error) {
	err := io.write2Way(KEY_USER, strconv.Itoa(uid), KEY_TASK, strconv.Itoa(tid), updateAttrs)
	return err
}

func (io *dataio)DelUserTask(uid int, tid int) (error) {
	err := io.del2Way(KEY_USER, strconv.Itoa(uid), KEY_TASK, strconv.Itoa(tid))
	return err
}
