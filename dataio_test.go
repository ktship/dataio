package dataio

import (
	"testing"
	. "gopkg.in/check.v1"
	"log"
	"time"
)


const TEST_TABLE_NAME_USERS = "test_users"
const TEST_TABLE_NAME_ACCOUNTS = "test_accounts"
const TEST_CACHE_NAME_USERS = "u"

func Test(t *testing.T) { TestingT(t) }

type TableSuite struct {
	io 	*dataio
}

func (s *TableSuite) SetUpSuite(c *C) {
	list, err := s.io.Ddbio.listTables()
	if err != nil {
		c.Fatal(err)
	}
	if s.io.Ddbio.isExistTableByName(list.TableNames, TEST_TABLE_NAME_USERS) {
		if err := s.io.Ddbio.deleteTable(TEST_TABLE_NAME_USERS) ; err != nil {
			c.Fatal(err)
		}
	}
	if s.io.Ddbio.isExistTableByName(list.TableNames, TEST_TABLE_NAME_ACCOUNTS) {
		if err := s.io.Ddbio.deleteTable(TEST_TABLE_NAME_ACCOUNTS) ; err != nil {
			c.Fatal(err)
		}
	}

	if err = s.io.Ddbio.createHashTable(TEST_TABLE_NAME_USERS, 1, 1) ; err != nil {
		c.Fatal(err)
	}
	if err = s.io.Ddbio.createHashTable(TEST_TABLE_NAME_ACCOUNTS, 1, 1) ; err != nil {
		c.Fatal(err)
	}

	s.io.Ddbio.waitUntilStatus(TEST_TABLE_NAME_USERS, "ACTIVE")

	s.io.cio.FlushDB()
	s.io.cio.SetTTL(10)
}
func (s *TableSuite) SetUpTest(c *C) {
}
func (s *TableSuite) TearDownTest(c *C) {
}
func (s *TableSuite) TearDownSuite(c *C) {
	if err := s.io.Ddbio.deleteTable(TEST_TABLE_NAME_USERS) ; err != nil {
		c.Fatal(err)
	}
}

var _ = Suite(&TableSuite {
	io : New(),
})

func (s *TableSuite) Test001_DynamoDBIO(c *C) {
	log.Println("# Tests to DynamoDB read/write item")

	// Test Data
	tt := time.Now().Unix()
	data1 := map[string]interface{} {
		"createTime":int(time.Now().Unix()),
		"greeting": "hello",
	}
	data2 := map[string]interface{} {
		"greeting": "hello 2",
		"ac": "test",
		"b": 1234,
	}

	// 일단 데이터를 씀.
	var err error
	err = s.io.Ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "111", "", "", data1)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.io.Ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "222", "", "", data1)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.io.Ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "333", "", "", data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// 1차적으로 쓴 내용 확인.
	resp, errRead := s.io.Ddbio.readHashItem(TEST_TABLE_NAME_USERS, "111", "", "")
	if (errRead != nil) {
		c.Fatal(err)
	}
	if (resp["createTime"] != int(tt)) {
		c.Fatalf(" createTime(%d) is not %d... type: %T", resp["createTime"], tt, resp["createTime"])
	}
	if (resp["greeting"] != "hello") {
		c.Fatalf(" greeting(%s) is not tt... type: %T", resp["greeting"], resp["greeting"])
	}

	// 2차적으로 데이터 갱신
	err = s.io.Ddbio.writeHashItem(TEST_TABLE_NAME_USERS, "111", "", "", data2)
	if (err != nil) {
		c.Fatal(err)
	}

	// 2차적으로 갱신한 데이터 확인
	resp, errRead = s.io.Ddbio.readHashItem(TEST_TABLE_NAME_USERS, "111", "", "")
	if (errRead != nil) {
		c.Fatal(err)
	}
	if (resp["createTime"] != int(tt)) {
		c.Fatalf(" createTime(%d) is not %d... type: %T", resp["createTime"], tt, resp["createTime"])
	}
	if (resp["greeting"] != "hello 2") {
		c.Fatalf(" str(%s) is not test...", resp["greeting"])
	}
	if (resp["ac"] != "test") {
		c.Fatalf(" str(%s) is not test...", resp["greeting2"])
	}
	if (resp["b"] != 1234) {
		c.Fatalf(" str(%s) is not test...", resp["greeting2"])
	}
}

func (s *TableSuite) Test002_CacheIO_BASE(c *C) {
	log.Println("# Tests to Cache Redis read/write item")

	// Test Data
	tt := time.Now().Unix()
	data1 := map[string]interface{} {
		"createTime":time.Now().Unix(),
		"s_greeting": "hello",
	}
	data2 := map[string]interface{} {
		"greeting": "hello 2",
		"ac": "test",
		"b": 1234,
	}

	// 일단 데이터를 씀.
	var err error
	err = s.io.cio.writeHashItem(TEST_CACHE_NAME_USERS, "111", "", "", data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	resp, errRead := s.io.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
	if (errRead != nil) {
		c.Fatal(errRead)
	}

	if (resp["createTime"] != int(tt)) {
		c.Fatalf(" createTime(%d) is not %d... type: %T", tt, resp["createTime"])
	}
	if (resp["s_greeting"] != "hello") {
		c.Fatalf(" greeting(%s) is not tt... type: %T", resp["s_greeting"], resp["s_greeting"])
	}


	// 2차적으로 데이터 갱신
	err = s.io.cio.writeHashItem(TEST_CACHE_NAME_USERS, "111", "", "", data2)
	if (err != nil) {
		c.Fatal(err)
	}
	{
		// 2차적으로 갱신한 데이터 확인
		resp, errRead := s.io.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
		if (errRead != nil) {
			c.Fatal(err)
		}
		if (resp["createTime"] != int(tt)) {
			c.Fatalf(" str(%s) is not test...", resp["greeting"])
		}
		if (resp["greeting"] != "hello 2") {
			c.Fatalf(" str(%s) is not test...", resp["greeting"])
		}
		if (resp["ac"] != "test") {
			c.Fatalf(" str(%s) is not test...", resp["test"])
		}
		if (resp["b"] != 1234) {
			c.Fatalf(" str(%s) is not test...", resp["test"])
		}
	}
}

func (s *TableSuite) Test003_CacheIO_TTL(c *C) {
	log.Println("# Tests to TTL Cache Redis read/write item")

	// Test Data
	tt := time.Now().Unix()
	data1 := map[string]interface{} {
		"createTime":time.Now().Unix(),
		"s_greeting": "hello",
	}

	// 일단 데이터를 씀.
	var err error
	err = s.io.cio.writeHashItem(TEST_CACHE_NAME_USERS, "111", "", "", data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	resp, errRead := s.io.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
	if (errRead != nil) {
		c.Fatal(errRead)
	}
	if (resp["createTime"] != int(tt)) {
		c.Fatalf(" createTime(%d) is not %s...", tt, resp["createTime"])
	}
	if (resp["s_greeting"] != "hello") {
		c.Fatalf(" greeting(%s) is not tt... type: %T", resp["s_greeting"], resp["s_greeting"])
	}

	time.Sleep(time.Second * (time.Duration)(s.io.cio.GetTTL() + 1))

	// Expire 된 키는 소멸되어야함. resp nil체크
	resp, errRead = s.io.cio.readHashItem(TEST_CACHE_NAME_USERS, "111", "", "")
	if (errRead != nil) {
		log.Printf(" expired: %v", errRead)
	}
	if resp != nil {
		c.Fatalf(" Does NOT expired!!! WHY??? ")
	}
}

func (s *TableSuite) Test004_CacheIO_Hash(c *C) {
	log.Println("# Tests to TTL Cache Redis read/write Hash")

	// Test Data
	data1 := map[string]interface{} {
		"a": "hello",
		"b": 1234,
	}
	data2 := map[string]interface{} {
		"a": "hello2",
		"c": 1000000000,
	}

	// 일단 데이터를 씀.
	var err error
	err = s.io.cio.writeHashItem(KEY_USER, "000", KEY_TASK, "1", data1)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	resp, errRead := s.io.cio.readHashItem(KEY_USER, "000", KEY_TASK, "1")
	if (errRead != nil) {
		c.Fatal(errRead)
	}
	if (resp["a"] != "hello") {
		c.Fatalf("hello")
	}
	if (resp["b"] != 1234) {
		c.Fatalf("1234")
	}

	err = s.io.cio.writeHashItem(KEY_USER, "000", KEY_TASK, "1", data2)
	if (err != nil) {
		c.Fatal(err)
	}

	// cache 내용 읽기 --------------------
	resp, errRead = s.io.cio.readHashItem(KEY_USER, "000", KEY_TASK, "1")
	if (errRead != nil) {
		c.Fatal(errRead)
	}
	if (resp["a"] != "hello2") {
		c.Fatalf("hello")
	}
	if (resp["b"] != 1234) {
		c.Fatalf("1234")
	}
	if (resp["c"] != 1000000000) {
		c.Fatalf("1000000000")
	}
}

