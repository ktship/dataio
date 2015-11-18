package dataio

import (
	"fmt"
	"log"
	"time"
	"bytes"
	"strconv"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var config = &aws.Config {
	Endpoint: aws.String(URL_dynamoDB),
	Region: aws.String("us-west-2"),
	MaxRetries:              aws.Int(2),
}

type ddbio struct {
	db 	*dynamodb.DynamoDB
}

func NewDB() *ddbio {
	return &ddbio{
		db: dynamodb.New(session.New(), config),
	}
}

func (io *ddbio)readHashItem(hkey string, hid string, hkey2 string, hid2 string) (map[string]interface{}, error) {

	// 파라메터 구성
	var params *dynamodb.GetItemInput
	if hkey2 == "" {
		params = &dynamodb.GetItemInput{
			TableName: aws.String(hkey),
			Key: map[string]*dynamodb.AttributeValue{ // Required
				hkey: {
					S:    aws.String(hid),
				},
			},
			ConsistentRead: aws.Bool(true),
			//		ExpressionAttributeNames: map[string]*string{"Key": aws.String("AttributeName"), },
			//		ProjectionExpression:   aws.String("ProjectionExpression"),
			ReturnConsumedCapacity: aws.String("INDEXES"),
		}
	} else {
		params = &dynamodb.GetItemInput{
			TableName: aws.String(hkey2),
			Key: map[string]*dynamodb.AttributeValue{ // Required
				hkey: {
					S:    aws.String(hid),
				},
				hkey2: {
					S:    aws.String(hid2),
				},
			},
			ConsistentRead: aws.Bool(true),
			//		ExpressionAttributeNames: map[string]*string{"Key": aws.String("AttributeName"), },
			//		ProjectionExpression:   aws.String("ProjectionExpression"),
			ReturnConsumedCapacity: aws.String("INDEXES"),
		}
	}

	resp, err := io.db.GetItem(params)

	if err != nil {
		log.Printf("DB ReadItemAll ERROR: %s \n", err)
		return nil, err
	}
	if (DEBUG_MODE_UNIT_CONSUMED_LOG) {
		log.Printf("Read consume unit: %v", resp.ConsumedCapacity)
	}
	// output map
	outMap := make(map[string]interface{})

	for k, v := range resp.Item {
		if (v.S != nil) {
			outMap[k] = *v.S
		} else if (v.N != nil) {
			ii, _ := strconv.ParseInt(*v.N, 10, 0)
			outMap[k] = int(ii)
		} else {
			log.Printf("DB ReadItemAll ERROR: unknown type of attr.. check! key: %s, value:%+v", k, v)
			return nil, fmt.Errorf("DB ReadItemAll ERROR: unknown type of attr.. check! key: %s, value:%+v", k, v)
		}
	}

	return outMap, nil
}

func (io *ddbio)writeHashItem(hkey string, hid string, hkey2 string, hid2 string, updateAttrs map[string]interface{}) (error) {
	exprValues := make(map[string]*dynamodb.AttributeValue)
	var buffer bytes.Buffer
	buffer.WriteString("set ")
	count := 0
	for k, v := range updateAttrs {
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
		default:
			_ = t
			log.Printf("DB ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
			return fmt.Errorf("DB ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
		}
		count++
	}

	updateExpr := buffer.String()

	if DEBUG_MODE_LOG {
		fmt.Printf("updateExpr : %s \n",updateExpr)
		fmt.Printf("exprValues : %v \n",exprValues)
	}

	var params *dynamodb.UpdateItemInput
	if hkey2 == "" {
		params = &dynamodb.UpdateItemInput{
			TableName: aws.String(hkey), // Required
			Key: map[string]*dynamodb.AttributeValue{
				hkey: {
					S:    aws.String(hid),
				},
			},
			//		ConditionExpression: aws.String("ConditionExpression"),
			ReturnConsumedCapacity:      aws.String("INDEXES"),
			ReturnItemCollectionMetrics: aws.String("SIZE"),
			// ALL_NEW, ALL_OLD, UPDATED_NEW, UPDATED_OLD, NONE
			ReturnValues:                aws.String("ALL_OLD"), // The old versions of only the updated attributes are returned.
			UpdateExpression:            aws.String(updateExpr),
			ExpressionAttributeValues: 	 exprValues,
		}
	} else {
		params = &dynamodb.UpdateItemInput{
			TableName: aws.String(hkey2), // Required
			Key: map[string]*dynamodb.AttributeValue{
				hkey: {
					S:    aws.String(hid),
				},
				hkey2: {
					S:    aws.String(hid2),
				},
			},
			//		ConditionExpression: aws.String("ConditionExpression"),
			ReturnConsumedCapacity:      aws.String("INDEXES"),
			ReturnItemCollectionMetrics: aws.String("SIZE"),
			// ALL_NEW, ALL_OLD, UPDATED_NEW, UPDATED_OLD, NONE
			ReturnValues:                aws.String("ALL_OLD"), // The old versions of only the updated attributes are returned.
			UpdateExpression:            aws.String(updateExpr),
			ExpressionAttributeValues: 	 exprValues,
		}
	}

	resp, err := io.db.UpdateItem(params)

	if err != nil {
		log.Printf("DB ERROR: %s \n", err)
		return err
	}
	// Pretty-print the response data.
	if DEBUG_MODE_LOG {
		log.Println("WriteItemAttributes ----");
		log.Println(resp)
	}
	return nil
}


func (io *ddbio)delHashItem(hkey string, hid string, hkey2 string, hid2 string) (error) {
	var params *dynamodb.DeleteItemInput
	if hkey2 == "" {
		params = &dynamodb.DeleteItemInput{
			TableName: aws.String(hkey), // Required
			Key: map[string]*dynamodb.AttributeValue{
				hkey: {
					S:    aws.String(hid),
				},
			},
			//		ConditionExpression: aws.String("ConditionExpression"),
			ReturnConsumedCapacity:      aws.String("INDEXES"),
			ReturnItemCollectionMetrics: aws.String("SIZE"),
			// ALL_OLD, NONE
			ReturnValues:                aws.String("ALL_OLD"), // The old versions of only the updated attributes are returned.
		}
	} else {
		params = &dynamodb.DeleteItemInput{
			TableName: aws.String(hkey), // Required
			Key: map[string]*dynamodb.AttributeValue{
				hkey: {
					S:    aws.String(hid),
				},
				hkey2: {
					S:    aws.String(hid2),
				},
			},
			//		ConditionExpression: aws.String("ConditionExpression"),
			ReturnConsumedCapacity:      aws.String("INDEXES"),
			ReturnItemCollectionMetrics: aws.String("SIZE"),
			//  ALL_OLD, NONE
			ReturnValues:                aws.String("ALL_OLD"), // The old versions of only the updated attributes are returned.
		}
	}

	resp, err := io.db.DeleteItem(params)

	if err != nil {
		log.Printf("DB ERROR: %s \n", err)
		return err
	}
	// Pretty-print the response data.
	if DEBUG_MODE_LOG {
		log.Println("DeleteItem ----");
		log.Println(resp)
	}
	return nil
}

// -------------------------------------------------
// user
// -------------------------------------------------
func (io *ddbio)ReadUser(uid string) (map[string]interface{}, error) {
	resp, err := io.readHashItem(KEY_CACHE_USER, uid, "", "")
	return resp, err
}

func (io *ddbio)WriteUser(uid string, updateAttrs map[string]interface{}) (error) {
	err := io.writeHashItem(KEY_CACHE_USER, uid, "", "", updateAttrs)
	return err
}

// -------------------------------------------------
// user : task
// -------------------------------------------------
func (io *ddbio)ReadUserTask(uid string, tid string) (map[string]interface{}, error) {
	resp, err := io.readHashItem(KEY_CACHE_USER, uid, KEY_CACHE_TASK, tid)
	return resp, err
}

func (io *ddbio)WriteUserTask(uid string, tid string, updateAttrs map[string]interface{}) (error) {
	err := io.writeHashItem(KEY_CACHE_USER, uid, KEY_CACHE_TASK, tid, updateAttrs)
	return err
}

func (io *ddbio)DelUserTask(uid string, tid string) (error) {
	err := io.delHashItem(KEY_CACHE_USER, uid, KEY_CACHE_TASK, tid)
	return err
}

// Table API ----------------------
// pkey 가 테이블 이름
func (io *ddbio)CreateHashTable( pkey string, readCap int, writeCap int) error {
	params := &dynamodb.CreateTableInput {
		TableName: aws.String(pkey),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(pkey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(pkey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(readCap)),
			WriteCapacityUnits: aws.Int64(int64(writeCap)),
		},
	}
	resp, err := io.db.CreateTable(params)
	if err != nil {
		log.Printf("DB CreateCounterTable ERROR: %s \n", err)
		if DEBUG_MODE_LOG { log.Println(err.Error()) }
		return err
	}
	if DEBUG_MODE_LOG { log.Println(resp) }
	return nil
}

// pRange 가 테이블 이름
func (io *ddbio)CreateHashRangeTable(pkey string, pRange string, readCap int, writeCap int) error {
	params := &dynamodb.CreateTableInput {
		TableName: aws.String(pRange),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(pkey),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(pRange),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(pkey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(pRange),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(readCap)),
			WriteCapacityUnits: aws.Int64(int64(writeCap)),
		},
	}
	resp, err := io.db.CreateTable(params)
	if err != nil {
		log.Printf("DB CreateCounterTable ERROR: %s \n", err)
		if DEBUG_MODE_LOG { log.Println(err.Error()) }
		return err
	}
	if DEBUG_MODE_LOG { log.Println(resp) }
	return nil
}


func (io *ddbio)ListTables() (*dynamodb.ListTablesOutput, error) {
	params := &dynamodb.ListTablesInput {}
	if tables, err := io.db.ListTables(params); err != nil {
		log.Println(err)
		return tables, err
	} else {
		log.Printf("tables: %v \n", tables)
		return tables, nil
	}
}

func (io *ddbio)DescribeTable(tableName string) (*dynamodb.DescribeTableOutput, error) {
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	resp, err := io.db.DescribeTable(params)

	if err != nil {
		log.Printf("DB DescribeTable ERROR: %s \n", err)
		if DEBUG_MODE_LOG { log.Println(err.Error()) }
		return resp, err
	}

	if DEBUG_MODE_LOG { log.Println(resp) }
	return resp, nil
}

func (io *ddbio)DeleteTable(tableName string) error {
	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	resp, err := io.db.DeleteTable(params)

	if err != nil {
		log.Printf("DB DeleteTable ERROR: %s \n", err)
		if DEBUG_MODE_LOG { log.Println(err.Error()) }
		return err
	}

	// Pretty-print the response data.
	if DEBUG_MODE_LOG { log.Println(resp) }
	return nil
}

func (ddbio *ddbio) WaitUntilStatus(tableName string, status string) {
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
		log.Printf("Expect a status to be %s, but timed out", status)
		close(done)
	}
}

func (ddbio *ddbio)isExistTableByName(tables []*string, name string) bool {
	for _, t := range tables {
		if *t == name {
			return true
		}
	}
	return false
}

func (ddbio *ddbio)GetTableStatus(tableName string) (*string, error) {
	desc, err := ddbio.DescribeTable(tableName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return desc.Table.TableStatus, nil
}
