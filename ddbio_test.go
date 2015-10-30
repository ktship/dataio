package dataio

import (
	"testing"
	. "gopkg.in/check.v1"
	"fmt"
	"log"
	"time"
)


func Test(t *testing.T) { TestingT(t) }

type TableSuite struct {
	ddbio *Ddbio
}

func (s *TableSuite) SetUpSuite(c *C) {
	s.ddbio = NewDB()

	list, err := s.ddbio.ListTables()
	if err != nil {
		c.Fatal(err)
	}
	if s.ddbio.isExistTableByName(list.TableNames, TABLE_NAME_USERS) {
		if err := s.ddbio.DeleteTable(TABLE_NAME_USERS) ; err != nil {
			c.Fatal(err)
		}
	}
	if s.ddbio.isExistTableByName(list.TableNames, TABLE_NAME_COUNTER) {
		if err := s.ddbio.DeleteTable(TABLE_NAME_COUNTER) ; err != nil {
			c.Fatal(err)
		}
	}
	if s.ddbio.isExistTableByName(list.TableNames, TABLE_NAME_ACCOUNTS) {
		if err := s.ddbio.DeleteTable(TABLE_NAME_ACCOUNTS) ; err != nil {
			c.Fatal(err)
		}
	}

	if err = s.ddbio.CreateCounterTable() ; err != nil {
		c.Fatal(err)
	}
	if err = s.ddbio.CreateUserTable() ; err != nil {
		c.Fatal(err)
	}
	if err = s.ddbio.CreateAccountTable() ; err != nil {
		c.Fatal(err)
	}

	s.ddbio.WaitUntilStatus(TABLE_NAME_USERS, "ACTIVE")
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

var _ = Suite(&TableSuite {})

func (s *TableSuite) Test001_BasicReadWrite(c *C) {
	log.Println("# Tests to read/write item")

	// 일단 데이터를 씀.
	tt := time.Now().Unix()
	data := map[string]interface{} {
		"createTime":tt,
		"greeting": "hello",
	}
	newMap := map[string]interface{} {
		"zzz": map[string]interface{} {
			"a": "test",
			"b": 1234,
		},
	}
	var err error
	err = s.ddbio.WriteItemAttributes("users", "uid", "111", data, newMap)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.ddbio.WriteItemAttributes("users", "uid", "222", data, nil)
	if (err != nil) {
		c.Fatal(err)
	}

	err = s.ddbio.WriteItemAttributes("users", "uid", "333", nil, newMap)
	if (err != nil) {
		c.Fatal(err)
	}

	// -----
	resp, errRead := s.ddbio.ReadItemAll("users", "uid", "111")
	if (errRead != nil) {
		c.Fatal(err)
	}

	if (resp["createTime"] != int(tt)) {
		c.Fatalf(" createTime(%d) is not %d... type: %T", resp["createTime"], tt, resp["createTime"])
	}
	if (resp["greeting"] != "hello") {
		c.Fatalf(" greeting(%s) is not tt... type: %T", resp["greeting"], resp["greeting"])
	}

	// 1차적으로 쓴 내용 확인.
	zzz := resp["zzz"].(map[string]interface{})
	str := zzz["a"]
	intb := zzz["b"]
	if (str != "test") {
		c.Fatalf(" str(%s) is not test...", str)
	}
	if (intb != 1234) {
		c.Fatalf(" dd(%d) is not 1234... type: %T", intb, intb)
	}

	// 2차적으로 데이터 갱신

	// 2차적으로 갱신한 데이터 확인

}

func (s *TableSuite) Test002(c *C) {
	log.Println("Test002 ---")


}



