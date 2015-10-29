package dataio

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"time"
	"bytes"
	"strconv"
)

var config = &aws.Config {
	Endpoint: aws.String(URL_dynamoDB),
	Region: aws.String("us-west-2"),
	MaxRetries:              aws.Int(2),
}

type Ddbio struct {
	db 	*dynamodb.DynamoDB
}

func NewDB() *Ddbio {
	return &Ddbio{
		db: dynamodb.New(config),
	}
}

// API -----------------------

/*
func (io *Ddbio)ReadUserData(uid string) (map[string]string, error) {
	dat := new(map[string]string)

	return dat, nil;
}

func (io *Ddbio)WriteUserData(id string, idSub string, dat map[string]string) error {
	return nil;
}

func (io *Ddbio)DeleteUserData(id string, idSub string) error {
	return nil;
}
*/
// Item API -----------------------
func (io *Ddbio)ReadItemAll(tableName string, keyName string, keyValue string) (*dynamodb.GetItemOutput, error) {
	params := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{ // Required
			keyName: {
				S:    aws.String(keyValue),
			},
		},
		ConsistentRead: aws.Bool(true),
//		ExpressionAttributeNames: map[string]*string{"Key": aws.String("AttributeName"), },
//		ProjectionExpression:   aws.String("ProjectionExpression"),
		ReturnConsumedCapacity: aws.String("INDEXES"),
	}
	resp, err := io.db.GetItem(params)

	if err != nil {
		fmt.Println(err.Error())
		return resp, err
	}

	fmt.Println(resp)
	return resp, nil
}

func (io *Ddbio)WriteItemAttributes(tableName string, keyName string, keyValue string, attrs map[string]interface{}) (error) {
	exprValues := make(map[string]*dynamodb.AttributeValue)
	var buffer bytes.Buffer
	buffer.WriteString("set ")
	count := 0
	for k, v := range attrs {
		val := fmt.Sprintf(":v%d", count)
		if count != 0 { buffer.WriteString(", ") }
		switch t := v.(type) {
		case string:
			buffer.WriteString(fmt.Sprintf("%s = %s", k, val))
			exprValues[val] = &dynamodb.AttributeValue { S: aws.String(v.(string)), }
		case int:
			buffer.WriteString(fmt.Sprintf("%s = %s", k, val))
			itoa := strconv.Itoa(v.(int))
			exprValues[val] = &dynamodb.AttributeValue { N: aws.String(itoa), }
		case int64:
			itoa := strconv.FormatInt(v.(int64), 10)
			buffer.WriteString(fmt.Sprintf("%s = %s", k, val))
			exprValues[val] = &dynamodb.AttributeValue { N: aws.String(itoa), }
		case map[string]interface{}:
			subCount := 0
			for kk, vv := range v.(map[string]interface{}) {
				subVal := fmt.Sprintf(":v%ds%d", count, subCount)
				if subCount != 0 { buffer.WriteString(", ") }
				switch tt := vv.(type) {
				case string:
					buffer.WriteString(fmt.Sprintf("%s.%s = %s", k, kk, subVal))
					exprValues[subVal] = &dynamodb.AttributeValue{S: aws.String(vv.(string)), }
				case int:
					buffer.WriteString(fmt.Sprintf("%s.%s = %s", k, kk, subVal))
					itoa := strconv.Itoa(vv.(int))
					exprValues[subVal] = &dynamodb.AttributeValue { N: aws.String(itoa), }
				case int64:
					itoa := strconv.FormatInt(vv.(int64), 10)
					buffer.WriteString(fmt.Sprintf("%s.%s = %s", k, kk, subVal))
					exprValues[subVal] = &dynamodb.AttributeValue { N: aws.String(itoa), }
				default:
					_ = tt
					return fmt.Errorf("unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
				}
				subCount++
			}
		default:
			_ = t
			return fmt.Errorf("unknown type of attribute.. check key: %s, value:%+v", k, v)
		}
		count++
	}
	updateExpr := buffer.String()
	fmt.Printf("updateExpr : %s \n",updateExpr)

	params := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName), // Required
		Key: map[string]*dynamodb.AttributeValue{
			keyName: {
				S:    aws.String(keyValue),
			},
		},
//		ConditionExpression: aws.String("ConditionExpression"),
		ReturnConsumedCapacity:      aws.String("INDEXES"),
		ReturnItemCollectionMetrics: aws.String("SIZE"),
		// ALL_NEW, ALL_OLD, UPDATED_NEW, UPDATED_OLD, NONE
		ReturnValues:                aws.String("ALL_NEW"), // The old versions of only the updated attributes are returned.
		UpdateExpression:            aws.String(updateExpr),
		ExpressionAttributeValues: 	 exprValues,
	}
	resp, err := io.db.UpdateItem(params)

	if err != nil {
		return err
	}
	// Pretty-print the response data.
	if true { fmt.Println("WriteItemAttributes ----"); fmt.Println(resp) }
	return nil
}

// Table API ----------------------

func (io *Ddbio)CreateCounterTable() error {
	// JSON 파라메터
	// 기본키 : name (string) :카운터의 이름. "uid", "payload"등이 있음.
	// 쓰루풋 : 인 1, 아웃 1
	params := &dynamodb.CreateTableInput {
		TableName: aws.String(TABLE_NAME_COUNTER),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("name"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("name"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	resp, err := io.db.CreateTable(params)
	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return err
	}
	if DEBUG_MODE { fmt.Println(resp) }
	return nil
}

func (io *Ddbio)CreateAccountTable() error {
	// JSON 파라메터
	// 기본키 : name (string) :카운터의 이름. "uid", "payload"등이 있음.
	// 쓰루풋 : 인 1, 아웃 1
	params := &dynamodb.CreateTableInput {
		TableName: aws.String(TABLE_NAME_ACCOUNTS),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("name"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("name"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	resp, err := io.db.CreateTable(params)
	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return err
	}
	if DEBUG_MODE { fmt.Println(resp) }
	return nil
}

func (io *Ddbio)CreateUserTable() error {
	// JSON 파라메터
	// 기본키 : uid (string)
	// 쓰루풋 : 인 1, 아웃 1
	params := &dynamodb.CreateTableInput {
		TableName: aws.String(TABLE_NAME_USERS),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("uid"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("uid"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}
	resp, err := io.db.CreateTable(params)
	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return err
	}
	if DEBUG_MODE { fmt.Println(resp) }
	return nil
}

func (io *Ddbio)ListTables() (*dynamodb.ListTablesOutput, error) {
	params := &dynamodb.ListTablesInput {}
	if tables, err := io.db.ListTables(params); err != nil {
		fmt.Println(err)
		return tables, err
	} else {
		log.Printf("tables: %v \n", tables)
		return tables, nil
	}
}

func (io *Ddbio)DescribeTable(tableName string) (*dynamodb.DescribeTableOutput, error) {
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	resp, err := io.db.DescribeTable(params)

	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return resp, err
	}

	if DEBUG_MODE { fmt.Println(resp) }
	return resp, nil
}

func (io *Ddbio)DeleteTable(tableName string) error {
	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	resp, err := io.db.DeleteTable(params)

	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return err
	}

	// Pretty-print the response data.
	if DEBUG_MODE { fmt.Println(resp) }
	return nil
}

func (ddbio *Ddbio) WaitUntilStatus(tableName string, status string) {
	// We should wait until the table is in specified status because a real DynamoDB has some delay for ready
	done := make(chan bool)
	timeout := time.After(TIMEOUT)
	go func() {
		for {
			select {
			case <-done:
				log.Println("channel done is closed")
				return
			default:
				desc, err := ddbio.DescribeTable(tableName)
				if err != nil {
					log.Fatal(err)
				}
				if *desc.Table.TableStatus == status {
					done <- true
					return
				}
				time.Sleep(5 * time.Second)
			}
		}
	}()
	select {
	case <-done:
		break
	case <-timeout:
		log.Fatalf("Expect a status to be %s, but timed out", status)
		close(done)
	}
}

func (ddbio *Ddbio)isExistTableByName(tables []*string, name string) bool {
	for _, t := range tables {
		if *t == name {
			return true
		}
	}
	return false
}

func (ddbio *Ddbio)GetTableStatus(tableName string) *string {
	desc, err := ddbio.DescribeTable(tableName)
	if err != nil {
		log.Fatal(err)
	}
	return desc.Table.TableStatus
}
