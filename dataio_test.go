package dataio

import (
	"testing"
	. "gopkg.in/check.v1"
	"fmt"
	"log"
	"time"
	"strconv"
)


func Test(t *testing.T) { TestingT(t) }

type TableSuite struct {
	ddbio 	*Ddbio
	cio   	*cio
	// 테스트 데이터
	tt 		int64
	data 	map[string]interface{}
	nMap 	map[string]interface{}
	newData map[string]interface{}
}

func (s *TableSuite) SetUpSuite(c *C) {

	list, err := s.ddbio.ListTables()
	if err != nil {
		c.Fatal(err)
	}
	if s.ddbio.isExistTableByName(list.TableNames, TABLE_NAME_USERS) {
		if err := s.ddbio.DeleteTable(TABLE_NAME_USERS) ; err != nil {
			c.Fatal(err)
		}
	}
	if s.ddbio.isExistTableByName(list.TableNames, TABLE_NAME_ACCOUNTS) {
		if err := s.ddbio.DeleteTable(TABLE_NAME_ACCOUNTS) ; err != nil {
			c.Fatal(err)
		}
	}

	if err = s.ddbio.CreateUserTable() ; err != nil {
		c.Fatal(err)
	}
	if err = s.ddbio.CreateAccountTable() ; err != nil {
		c.Fatal(err)
	}

	s.ddbio.WaitUntilStatus(TABLE_NAME_USERS, "ACTIVE")

	s.cio.FlushAll()
}
func (s *TableSuite) SetUpTest(c *C) {
	fmt.Printf("SetUpTest...  \n")
}
func (s *TableSuite) TearDownTest(c *C) {
	fmt.Printf("TearDownTest...  \n")
}
func (s *TableSuite) TearDownSuite(c *C) {
	fmt.Printf("TearDownSuite...  \n")

	if err := s.ddbio.DeleteTable(TABLE_NAME_USERS) ; err != nil {
		c.Fatal(err)
	}
}

var _ = Suite(&TableSuite {
	ddbio : NewDB(),
	cio 	: NewCache(),

	tt 		: time.Now().Unix(),
	data 	: map[string]interface{} {
		"createTime":time.Now().Unix(),
		"s_greeting": "hello",
	},
	nMap 	: map[string]interface{} {
		"zzz": map[string]interface{} {
			"ac": "test",
			"b": 1234,
		},
	},
	newData : map[string]interface{} {
		"greeting": "hello 2",
		"greeting2": "hi hi",
		"zzz": map[string]interface{} {
			"ac": "new test",
			"b": 1234,
			"c": "ccccccc",
			"d": 321321,
		},
	},
})

func (s *TableSuite) Test001_DynamoDBIO(c *C) {
	log.Println("# Tests to DynamoDB read/write item")

	// 일단 데이터를 씀.
	var err error
	err = s.ddbio.WriteItemAttributes("users", "uid", "111", s.data, s.nMap)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.ddbio.WriteItemAttributes("users", "uid", "222", s.data, nil)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.ddbio.WriteItemAttributes("users", "uid", "333", nil, s.nMap)
	if (err != nil) {
		c.Fatal(err)
	}

	// 1차적으로 쓴 내용 확인.
	resp, errRead := s.ddbio.ReadItemAll("users", "uid", "111")
	if (errRead != nil) {
		c.Fatal(err)
	}
	if (resp["createTime"] != int(s.tt)) {
		c.Fatalf(" createTime(%d) is not %d... type: %T", resp["createTime"], s.tt, resp["createTime"])
	}
	if (resp["s_greeting"] != "hello") {
		c.Fatalf(" greeting(%s) is not tt... type: %T", resp["greeting"], resp["greeting"])
	}
	zzz := resp["zzz"].(map[string]interface{})
	str := zzz["ac"]
	intb := zzz["b"]
	if (str != "test") {
		c.Fatalf(" str(%s) is not test...", str)
	}
	if (intb != 1234) {
		c.Fatalf(" dd(%d) is not 1234... type: %T", intb, intb)
	}
	

	// 2차적으로 데이터 갱신
	err = s.ddbio.WriteItemAttributes("users", "uid", "111", s.newData, nil)
	if (err != nil) {
		c.Fatal(err)
	}

	// 2차적으로 갱신한 데이터 확인
	resp, errRead = s.ddbio.ReadItemAll("users", "uid", "111")
	if (errRead != nil) {
		c.Fatal(err)
	}
	if (resp["greeting"] != "hello 2") {
		c.Fatalf(" str(%s) is not test...", resp["greeting"])
	}
	if (resp["greeting2"] != "hi hi") {
		c.Fatalf(" str(%s) is not test...", resp["greeting2"])
	}
	zzz2 := resp["zzz"].(map[string]interface{})
	if (zzz2["ac"] != "new test") {
		c.Fatalf(" str(%s) is not test...", zzz2["a"])
	}
	if (zzz2["b"] != 1234) {
		c.Fatalf(" dd(%d) is not test...", zzz2["b"])
	}
	if (zzz2["c"] != "ccccccc") {
		c.Fatalf(" dd(%s) is not test...", zzz2["c"])
	}
	if (zzz2["d"] != 321321) {
		c.Fatalf(" dd(%d) is not test...", zzz2["d"])
	}
}

func (s *TableSuite) Test002_CacheIO(c *C) {
	log.Println("# Tests to Cache Redis read/write item")

	// 일단 데이터를 씀.
	var err error
	err = s.cio.WriteItemAttributes("u", "111", s.data, s.nMap)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	numKeys := []string {"createTime", "zzz:b"}
	strKeys := []string {"s_greeting", "zzz:ac"}
	resp, errRead := s.cio.ReadItems("u", "111", numKeys, strKeys)
	if (errRead != nil) {
		c.Fatal(errRead)
	}

	ct := resp["createTime"]
	if (ct != int(s.tt)) {
		c.Fatalf(" createTime(%d) is not %d... type: %T", ct, s.tt, resp["createTime"])
	}
	if (resp["s_greeting"] != "hello") {
		c.Fatalf(" greeting(%s) is not tt... type: %T", resp["s_greeting"], resp["s_greeting"])
	}
	if (resp["zzz:ac"] != "test") {
		c.Fatalf(" str(%s) is not test...", resp["zzz:ac"])
	}
	if (resp["zzz:b"] != 1234) {
		c.Fatalf(" dd(%d) is not 1234... type: %T", resp["zzz:b"])
	}

	// 2차적으로 데이터 갱신
	err = s.cio.WriteItemAttributes("u", "111", s.newData, nil)
	if (err != nil) {
		c.Fatal(err)
	}
	{
		// 2차적으로 갱신한 데이터 확인
		numKeys := []string {"zzz:b", "zzz:d", "fdsa", "ff", "zzz:c"}
		strKeys := []string {"greeting", "greeting2", "zzz:ac", "nobady", "createTime"}
		resp, errRead := s.cio.ReadItems("u", "111", numKeys, strKeys)
		if (errRead != nil) {
			c.Fatal(err)
		}
		log.Printf("resp : %v", resp)
		if (resp["createTime"] != strconv.Itoa(int(s.tt))) {
			c.Fatalf(" str(%s) is not test...", resp["greeting"])
		}
		if (resp["greeting"] != "hello 2") {
			c.Fatalf(" str(%s) is not test...", resp["greeting"])
		}
		if (resp["greeting2"] != "hi hi") {
			c.Fatalf(" str(%s) is not test...", resp["greeting2"])
		}
		if (resp["zzz:ac"] != "new test") {
			c.Fatalf(" str(%s) is not test...", resp["zzz:ac"])
		}
		if resp["zzz:b"] != 1234 {
			c.Fatalf(" dd(%d) is not test...", resp["zzz:b"])
		}
		if resp["zzz:c"] != "ccccccc" {
//			c.Fatalf(" dd(%s) is not test...", resp["zzz:c"])
		}
		if resp["zzz:d"] != 321321 {
			c.Fatalf(" dd(%d) is not test...", resp["zzz:d"])
		}
		if resp["ff"] != NULL_NUMBER {
			c.Fatalf(" resp[ff](%d) is not NULL_NUMBER...", resp["ff"])
		}
	}
}



