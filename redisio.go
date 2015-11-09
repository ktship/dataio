package dataio

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
	"bytes"
	"log"
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
			c, err := redis.Dial("tcp", URL_LOCAL_REDIS)
			if err != nil {
				panic(err.Error())
			}

			if TEST_LOCAL_REDIS {
				c.Do("SELECT", "6")
			}
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

func (io *cio)FlushAll() error {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")
	conn.Send("FLUSH", 6)
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

func (io *cio)ReadItems(category string, key string, numAttrs []string, strAttrs []string) (map[string]interface{}, error) {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")

	cacheKey :=fmt.Sprintf("%s:%s", category, key)
	params := make([]interface{}, 0)
	params = append(params, cacheKey)
	for _, v := range numAttrs {
		params = append(params, v)
	}
	for _, v := range strAttrs {
		params = append(params, v)
	}

	conn.Send("HMGET", params...)
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
		return nil, err
	}
	retMap := make(map[string]interface{})
	for ii, vv := range numAttrs {
		if out[ii] != nil {
			num, errr := strconv.Atoi(string(out[ii].([]byte)))
			if errr != nil {
				retMap[vv] = NULL_NUMBER
			} else {
				retMap[vv] = num
			}
		} else {
			retMap[vv] = NULL_NUMBER
		}
	}
	nextCount := len(numAttrs)
	for ii, vv := range strAttrs {
		if out[nextCount + ii] != nil {
			retMap[vv] = string(out[nextCount + ii].([]byte))
		} else {
			retMap[vv] = ""
		}
	}

	return retMap, err
}

func (io *cio)WriteItemAttributes(category string, key string, updateAttrs map[string]interface{}, newMap map[string]interface{}) (error) {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")

	var buffer bytes.Buffer
	cacheKey := fmt.Sprintf("%s:%s", category, key)
	for k, v := range updateAttrs {
		switch t := v.(type) {
		case string:
			conn.Send("HSET", cacheKey, k, v.(string))
		case int:
			conn.Send("HSET", cacheKey, k, v.(int))
		case int64:
			conn.Send("HSET", cacheKey, k, v.(int64))
		case map[string]interface{}:
			for kk, vv := range v.(map[string]interface{}) {
				switch tt := vv.(type) {
				case string:
					conn.Send("HSET", cacheKey, fmt.Sprintf("%s:%s", k, kk) , vv.(string))
				case int:
					conn.Send("HSET", cacheKey, fmt.Sprintf("%s:%s", k, kk), vv.(int))
				case int64:
					conn.Send("HSET", cacheKey, fmt.Sprintf("%s:%s", k, kk), vv.(int64))
				default:
					_ = tt
					log.Printf("Cache ERROR: unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
					return fmt.Errorf("Cache unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
				}
			}
		default:
			_ = t
			log.Printf("Cache ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
			return fmt.Errorf("Cache ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
		}
		buffer.Reset()
	}

	for k, v := range newMap {
		switch t := v.(type) {
		case map[string]interface{}:
			for kk, vv := range v.(map[string]interface{}) {
				switch tt := vv.(type) {
				case string:
					conn.Send("HSET", cacheKey, fmt.Sprintf("%s:%s", k, kk) , vv.(string))
				case int:
					conn.Send("HSET", cacheKey, fmt.Sprintf("%s:%s", k, kk), vv.(int))
				case int64:
					conn.Send("HSET", cacheKey, fmt.Sprintf("%s:%s", k, kk), vv.(int64))
				default:
					_ = tt
					log.Printf("Cache ERROR: unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
					return fmt.Errorf(" Cache unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
				}
			}
		default:
			_ = t
			log.Printf("Cache ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
			return fmt.Errorf("Cache ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
		}
		buffer.Reset()
	}

	// TTL
	conn.Send("EXPIRE", cacheKey, io.ttl);
	resp, err := conn.Do("EXEC")
	if DEBUG_MODE_LOG {	log.Println(resp) }
	return err
}











