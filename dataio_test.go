package dataio

import (
	"testing"
	. "gopkg.in/check.v1"
	"fmt"
	"log"
	"time"
	"strconv"
)


const TEST_TABLE_NAME_USERS = "test_users"
const TEST_TABLE_NAME_ACCOUNTS = "test_accounts"
const TEST_CACHE_NAME_USERS = "u"

func Test(t *testing.T) { TestingT(t) }

type TableSuite struct {
	ddbio 	*ddbio
	cio   	*cio
	// 테스트 데이터
	tt 		int64
	data1 	map[string]interface{}
	data2 	map[string]interface{}
	data3 	map[string]interface{}
}

func (s *TableSuite) SetUpSuite(c *C) {

	list, err := s.ddbio.ListTables()
	if err != nil {
		c.Fatal(err)
	}
	if s.ddbio.isExistTableByName(list.TableNames, TEST_TABLE_NAME_USERS) {
		if err := s.ddbio.DeleteTable(TEST_TABLE_NAME_USERS) ; err != nil {
			c.Fatal(err)
		}
	}
	if s.ddbio.isExistTableByName(list.TableNames, TEST_TABLE_NAME_ACCOUNTS) {
		if err := s.ddbio.DeleteTable(TEST_TABLE_NAME_ACCOUNTS) ; err != nil {
			c.Fatal(err)
		}
	}

	if err = s.ddbio.CreateHashTable(TEST_TABLE_NAME_USERS, TEST_TABLE_NAME_USERS, 1, 1) ; err != nil {
		c.Fatal(err)
	}
	if err = s.ddbio.CreateHashTable(TEST_TABLE_NAME_ACCOUNTS, TEST_TABLE_NAME_USERS, 1, 1) ; err != nil {
		c.Fatal(err)
	}

	s.ddbio.WaitUntilStatus(TEST_TABLE_NAME_USERS, "ACTIVE")

	s.cio.FlushAll()
	s.cio.SetTTL(10)
}
func (s *TableSuite) SetUpTest(c *C) {
	fmt.Printf("SetUpTest...  \n")
}
func (s *TableSuite) TearDownTest(c *C) {
	fmt.Printf("TearDownTest...  \n")
}
func (s *TableSuite) TearDownSuite(c *C) {
	fmt.Printf("TearDownSuite...  \n")

	if err := s.ddbio.DeleteTable(TEST_TABLE_NAME_USERS) ; err != nil {
		c.Fatal(err)
	}
}

var _ = Suite(&TableSuite {
	ddbio : NewDB(),
	cio 	: NewCache(),

	tt 		: time.Now().Unix(),
	data1 	: map[string]interface{} {
		"createTime":time.Now().Unix(),
		"s_greeting": "hello",
	},
	data2 	: map[string]interface{} {
		"ac": "test",
		"b": 1234,
	},
	data3 : map[string]interface{} {
		"a": "hello 2",
		"b": "hi hi",
		"c": "new test",
		"d": 1234,
		"e": "ccccccc",
		"f": 321321,
	},
})

func (s *TableSuite) Test001_DynamoDBIO(c *C) {
	log.Println("# Tests to DynamoDB read/write item")
	c.Skip("abc")


	// 일단 데이터를 씀.
	var err error
	err = s.ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "111", "", "", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "222", "", "", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "333", "", "", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// 1차적으로 쓴 내용 확인.
	resp, errRead := s.ddbio.readHashItem(TEST_TABLE_NAME_USERS, "111", "", "")
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
	err = s.ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "111", "", "", s.data2)
	if (err != nil) {
		c.Fatal(err)
	}

	// 2차적으로 갱신한 데이터 확인
	resp, errRead = s.ddbio.readHashItem(TEST_TABLE_NAME_USERS, "111", "", "")
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

func (s *TableSuite) Test002_CacheIO_BASE(c *C) {
	log.Println("# Tests to Cache Redis read/write item")
	c.Skip("def")

	// 일단 데이터를 씀.
	var err error
	err = s.cio.writeHashItem(TEST_CACHE_NAME_USERS, "111", "", "", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	resp, errRead := s.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
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
	err = s.cio.writeHashItem(TEST_CACHE_NAME_USERS, "111", "", "", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}
	{
		// 2차적으로 갱신한 데이터 확인
		resp, errRead := s.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
		if (errRead != nil) {
			c.Fatal(err)
		}
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
		if resp["zzz:c"] != NULL_NUMBER {
			c.Fatalf(" dd(%s) is not test...", resp["zzz:c"])
		}
		if resp["zzz:d"] != 321321 {
			c.Fatalf(" dd(%d) is not test...", resp["zzz:d"])
		}
		if resp["ff"] != NULL_NUMBER {
			c.Fatalf(" resp[ff](%d) is not NULL_NUMBER...", resp["ff"])
		}
	}

	// 키가 없을때는 resp는 nil 이 와야함.
	{
		resp, errRead := s.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
		if (errRead != nil) {
			c.Fatal(errRead)
		}
		if resp != nil {
			c.Fatalf(" Does NOT expired!!! WHY??? ")
		}
	}
}

func (s *TableSuite) Test003_CacheIO_TTL(c *C) {
	log.Println("# Tests to TTL Cache Redis read/write item")
	c.Skip("wr")

	// 일단 데이터를 씀.
	var err error
	err = s.cio.writeHashItem(TEST_CACHE_NAME_USERS, "111", "", "", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	resp, errRead := s.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
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

	time.Sleep(time.Second * (time.Duration)(s.cio.GetTTL() + 1))

	// Expire 된 키는 소멸되어야함. resp nil체크
	resp, errRead = s.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
	if (errRead != nil) {
		c.Fatal(errRead)
	}
	if resp != nil {
		c.Fatalf(" Does NOT expired!!! WHY??? ")
	}
}

func (s *TableSuite) Test004_CacheIO_Hash(c *C) {
	log.Println("# Tests to TTL Cache Redis read/write Hash")

	// 일단 데이터를 씀.
	var err error
	err = s.cio.WriteUserTask("000", "1", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}
	log.Printf(" s.data : %v", s.data1)

	// cache 내용 읽기 --------------------
	resp, errRead := s.cio.ReadUserTask("000", "1")
	if (errRead != nil) {
		c.Fatal(errRead)
	}

	log.Printf(" resp : %v", resp)

	err = s.cio.WriteUserTask("111", "0", s.data1)
	if (err != nil) {
		c.Fatal(err)
	}
	log.Printf(" s.data : %v", s.data1)

	// cache 내용 읽기 --------------------
	resp, errRead = s.cio.ReadUserTask("111", "0")
	if (errRead != nil) {
		c.Fatal(errRead)
	}

	log.Printf(" resp : %v", resp)

}

