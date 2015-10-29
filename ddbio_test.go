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

func (s *TableSuite) Test001_CreateUser(c *C) {
	log.Println("# CreateUser")

	data := map[string]interface{} {
		"createTime": time.Now().Unix(),
		"zzz": map[string]interface{} {
			"a": "test",
			"b": "Fds",
		},
	}
	err := s.ddbio.WriteItemAttributes("users", "uid", "1234", data)
	if (err != nil) {
		log.Printf("%s \n", err)
	}
}

func (s *TableSuite) Test002(c *C) {
	log.Println("Test002 ---")


}



