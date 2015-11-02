package dataio

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
	"bytes"
	"log"
)

/*
	캐시에 저장되는 기본 포멧
	테이블 명:기본키명:기본키값:(레인지키명:레인지키값):실제 데이터(string, int, 해쉬)
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

}

func NewCache() *cio {
	return &cio{

	}
}

func (io *cio)ReadItems(tableName string, keyName string, keyValue string, attrs []string, hashAttrs []string) (map[string]interface{}, error) {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")

	var buffer bytes.Buffer
	for _, v := range attrs {
		buffer.WriteString(fmt.Sprintf("%s:%s:%s:%s", tableName, keyName, keyValue, v))
		cacheKey := buffer.String()
		conn.Send("GET", cacheKey)
		buffer.Reset()
	}
	for _, v := range hashAttrs {
		buffer.WriteString(fmt.Sprintf("%s:%s:%s:%s", tableName, keyName, keyValue, v))
		cacheKey := buffer.String()
		conn.Send("HGETALL", cacheKey)
		buffer.Reset()
	}
	resp, err := conn.Do("EXEC")
	if err != nil {
		return nil, err
	}

	out := resp.([]interface{})
	log.Printf("cio ReadItemAll : %v", out)
	log.Printf("cio ReadItemAll : %T", out)
	bb := []interface{} { 1, 2}
	log.Printf("bb : %T", bb[0])

	outMap := make(map[string]interface{})
	log.Printf("out : %T", out[1])
	log.Printf("out : %d", out[1])

	return outMap, nil
}

//	conn.Do("SET", "test:1:100", "fdsa")
//	conn.Do("HSET", "user:1001:jjj", "name", "fdsa")
func (io *cio)WriteItemAttributes(tableName string, keyName string, keyValue string, updateAttrs map[string]interface{}, newMap map[string]interface{}) (error) {
	conn := pool.Get()
	defer conn.Close()
	conn.Send("MULTI")

	var buffer bytes.Buffer
	for k, v := range updateAttrs {
		buffer.WriteString(fmt.Sprintf("%s:%s:%s:%s", tableName, keyName, keyValue, k))
		cacheKey := buffer.String()
		switch t := v.(type) {
		case string:
			conn.Send("SET", cacheKey, v.(string))
		case int:
			conn.Send("SET", cacheKey, v.(int))
		case int64:
			conn.Send("SET", cacheKey, v.(int64))
		case map[string]interface{}:
			for kk, vv := range v.(map[string]interface{}) {
				switch tt := vv.(type) {
				case string:
					conn.Send("HSET", cacheKey, kk, vv.(string))
				case int:
					conn.Send("HSET", cacheKey, kk, vv.(int))
				case int64:
					conn.Send("HSET", cacheKey, kk, vv.(int64))
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
		buffer.WriteString(fmt.Sprintf("%s:%s:%s:%s", tableName, keyName, keyValue, k))
		cacheKey := buffer.String()
		switch t := v.(type) {
		case map[string]interface{}:
			for kk, vv := range v.(map[string]interface{}) {
				switch tt := vv.(type) {
				case string:
					conn.Send("HSET", cacheKey, kk, vv.(string))
				case int:
					conn.Send("HSET", cacheKey, kk, vv.(int))
				case int64:
					conn.Send("HSET", cacheKey, kk, vv.(int64))
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

	resp, err := conn.Do("EXEC")

	log.Println(resp)

	return err

}











