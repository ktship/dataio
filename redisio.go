package dataio

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
	"strconv"
)

/*
	캐시에 저장되는 기본 포멧
	u:1012 Hash
	 +- 해시키-값
 */
func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle: 80,
		MaxActive: 12000, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", URL_REDIS)
			if err != nil {
				panic(err.Error())
			}
			c.Do("SELECT", NUM_REDIS_DB)
			return c, err
		},
	}
}

var pool = newPool()

type cio struct {
	ttl 	int
}

func NewCache() *cio {
	return &cio{
		ttl:	TTL_CACHE_USER_DATA,
	}
}

func (io *cio)FlushDB() error {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")
	conn.Send("FLUSH", NUM_REDIS_DB)
	_, err := conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (io *cio)GetTTL() int {
	return io.ttl
}

func (io *cio)SetTTL(sec int) {
	io.ttl = sec
}

// 형태의 키의 값들을 모두 읽음. 예를 들어 uid.task 의 특정 task 의 값들을 모두 읽음
func (io *cio)readHashItem(hkey string, hid string, hkey2 string, hid2 string) (map[string]interface{}, error) {
	conn := pool.Get()
	defer conn.Close()

	conn.Send("MULTI")
	var cacheKey string
	if hkey2 != "" {
		cacheKey =fmt.Sprintf("%s:%s:%s:%s", hkey, hid, hkey2, hid2)
	} else {
		cacheKey =fmt.Sprintf("%s:%s", hkey, hid)
	}

	conn.Send("HGETALL", cacheKey)
	conn.Send("EXPIRE", cacheKey, io.ttl);
	resp, err := conn.Do("EXEC")
	if err != nil {
		return nil, err
	}

	o := resp.([]interface{})
	// TODO: 왜 한겹 더 들어갔는지는 잘 모르겠음.
	out := o[0].([]interface{})
	// TODO: 인덱스 1 은 키 존재 유무를 알려주는 것 같음. 이게 맞는거 같지만.. 확인이 안되었음.
	isExist := o[1]
	if isExist == int64(0) {
		return nil, fmt.Errorf("NOT EXIST KEY")
	}
	retMap := make(map[string]interface{})
	for ii:=0 ; ii<len(out) ; ii = ii+2 {
		key := string(out[ii].([]byte))
		value := string(out[ii + 1].([]byte))
		valueInt, err := strconv.Atoi(value)
		if err != nil {
			retMap[key] = value
		} else {
			retMap[key] = valueInt
		}
	}

	return retMap, err
}

func (io *cio)writeHashItem(hkey string, hid string, hkey2 string, hid2 string, updateAttrs map[string]interface{}) (error) {
	conn := pool.Get()
	defer conn.Close()

	conn.Send("MULTI")

	var cacheKey string
	if hkey2 != "" {
		cacheKey =fmt.Sprintf("%s:%s:%s:%s", hkey, hid, hkey2, hid2)
	} else {
		cacheKey =fmt.Sprintf("%s:%s", hkey, hid)
	}

	// TODO: slice 생성 이거 맞나???
	params := make([]interface{}, 0)
	params = append(params, cacheKey)
	for kk, vv := range updateAttrs {
		params = append(params, kk)
		params = append(params, vv)
	}

	conn.Send("HMSET", params...)
	conn.Send("EXPIRE", cacheKey, io.ttl);
	_, err := conn.Do("EXEC")

	return err
}


func (io *cio)delHashItem(hkey string, hid string, hkey2 string, hid2 string) (error) {
	conn := pool.Get()
	defer conn.Close()

	conn.Send("MULTI")

	var cacheKey string
	if hkey != "" {
		cacheKey =fmt.Sprintf("%s:%s:%s:%s", hkey, hid, hkey2, hid2)
	} else {
		cacheKey =fmt.Sprintf("%s:%s", hkey, hid)
	}

	conn.Send("DEL", cacheKey)
	_, err := conn.Do("EXEC")

	return err
}











