package dataio

import (
	"testing"
	. "gopkg.in/check.v1"
	"fmt"
)

func Test(t *testing.T) { TestingT(t) }

type TableSuite struct {
	t int
}
func (s *TableSuite) TearDownSuite(c *C) {
	fmt.Printf("TearDownSuite...  \n")
}

var _ = Suite(&TableSuite {})

func (s *TableSuite) TestReturnError(c *C) {
	if err := CreateUserTable() ; err != nil {
		c.Fatal(err)
	}

}