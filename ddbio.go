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

type Ddbio struct {
	db 	*dynamodb.DynamoDB
}

func NewDB() *Ddbio {
	return &Ddbio{
		db: dynamodb.New(session.New(), config),
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
func (io *Ddbio)ReadItemAll(tableName string, keyName string, keyValue string) (map[string]interface{}, error) {
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
		} else if (v.M != nil) {
			subMap := make(map[string]interface{})
			for kk, vv := range v.M {
				if (vv.S != nil) {
					subMap[kk] = *vv.S
				} else if (vv.N != nil) {
					iii, _ := strconv.ParseInt(*vv.N, 10, 0)
					subMap[kk] = int(iii)
				}
			}
			outMap[k] = subMap
		} else {
			log.Printf("DB ReadItemAll ERROR: unknown type of attr.. check! key: %s, value:%+v", k, v)
			return nil, fmt.Errorf("DB ReadItemAll ERROR: unknown type of attr.. check! key: %s, value:%+v", k, v)
		}
	}

	return outMap, nil
}

/* 	WriteItemAttributes 주어진 테이블명, 기본키 를 가지고 attr를 갱신함. 맵 attr의 경우는 새로 생성도 함.
	*** 주의:
	1. 리스트, 셋 attr은 처리 하지 않음. 에러!
	2. 맵 attribute의 멤버를 수정할려고 할 시에는 반드시 해당 맵 attr가 미리 존재하고 있어야함. 파라메터4 참고.
	input :
		1. tableName string : 수정할 테이블 명
		2. keyName, keyValue : 기본 키(예, "uid", "1101")
		3. updateAttrs : 업데이트할 데이터들이 들어가는 값. 맵의 attr도 업데이트 가능하지만, 맵이 생성되어 있는 상태라야만 함.
		   				 즉, 없는 맵 Attr의 경우는 여기서 하지말고, newMap 으로 새 맵 attr을 만들어서 넣을 것.
		4. newMap : 이건 옵션. 오직 새 맵 attr을 새로 생성할 때만 사용. 이건 attr을 통채로 Overwrite하므로 꼭 생성할때만 사용할 것.
	output :
		error
 */
func (io *Ddbio)WriteItemAttributes(tableName string, keyName string, keyValue string, updateAttrs map[string]interface{}, newMap map[string]interface{}) (error) {
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
					buffer.WriteString(fmt.Sprintf("%s.%s = %s", k,
						kk, subVal))
					exprValues[subVal] = &dynamodb.AttributeValue{ S: aws.String(vv.(string)), }
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
					log.Printf("DB ERROR: unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
					return fmt.Errorf("unknown SUB type of attribute.. key: %s, value:%+v", kk, vv)
				}
				subCount++
			}
		default:
			_ = t
			log.Printf("DB ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
			return fmt.Errorf("DB ERROR: unknown type of attribute.. check key: %s, value:%+v", k, v)
		}
		count++
	}

	count = 0
	for k, v := range newMap {
		val := fmt.Sprintf(":nm%d", count)
		if len(exprValues) != 0 { buffer.WriteString(", ") }
		buffer.WriteString(fmt.Sprintf("%s = %s", k, val))
		switch t := v.(type) {
		case map[string]interface{}:
			newMapAttrs := make(map[string]*dynamodb.AttributeValue)
			var newMapAttr *dynamodb.AttributeValue
			for kk, vv := range v.(map[string]interface{}) {
				switch tt := vv.(type) {
				case string:
					newMapAttr = &dynamodb.AttributeValue{S: aws.String(vv.(string)), }
				case int:
					itoa := strconv.Itoa(vv.(int))
					newMapAttr = &dynamodb.AttributeValue{N: aws.String(itoa), }
				case int64:
					itoa := strconv.FormatInt(vv.(int64), 10)
					newMapAttr = &dynamodb.AttributeValue{N: aws.String(itoa), }
				default:
					_ = tt
					log.Printf("DB ERROR: unknown type of NEW MAP attribute.. check key: %s, value:%+v", kk, vv)
					return fmt.Errorf("DB ERROR: unknown type of NEW MAP attribute.. check key: %s, value:%+v", kk, vv)
				}
				newMapAttrs[kk] = newMapAttr
			}
			exprValues[val] = &dynamodb.AttributeValue{
				M: newMapAttrs,
			}
		default:
			_ = t
			log.Printf("DB ERROR: unknown type of NEW MAP.. check key: %s, value:%+v", k, v)
			return fmt.Errorf("DB ERROR: unknown type of NEW MAP.. check key: %s, value:%+v", k, v)
		}
	}

	updateExpr := buffer.String()

	if DEBUG_MODE_LOG {
		fmt.Printf("updateExpr : %s \n",updateExpr)
		fmt.Printf("exprValues : %v \n",exprValues)
	}

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

func (io *Ddbio)PutItem(tableName string, keyName string, keyValue string, attrs map[string]interface{}) (error) {
//	exprValues := make(map[string]*dynamodb.AttributeValue)
	if ( len(attrs) != 0 ) {
		return fmt.Errorf("PutItem is NOT implemented YET! :)")
	}

	params := &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item: map[string]*dynamodb.AttributeValue{
			keyName: {
				S:    aws.String(keyValue),
			},
			"zzz": {
				M: map[string]*dynamodb.AttributeValue{
					"Key": {
						S:    aws.String("value"),
					},
				},
			},
		},
//		ConditionExpression: aws.String("ConditionExpression"),
//		ExpressionAttributeNames: map[string]*string{"Key": aws.String("AttributeName"), },
//		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{		},
		ReturnConsumedCapacity:      aws.String("INDEXES"),
		ReturnItemCollectionMetrics: aws.String("SIZE"),
		ReturnValues:                aws.String("ALL_OLD"),
	}
	resp, err := io.db.PutItem(params)

	if err != nil {
		log.Printf("DB PutItem ERROR: %s \n", err)
		return err
	}
	if DEBUG_MODE_LOG { log.Println("PutItem ----"); log.Println(resp) }

	return nil
}

// Table API ----------------------

func (io *Ddbio)CreateHashTable(tabaleName string, pkey string, readCap int, writeCap int) error {
	params := &dynamodb.CreateTableInput {
		TableName: aws.String(tabaleName),
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

func (io *Ddbio)ListTables() (*dynamodb.ListTablesOutput, error) {
	params := &dynamodb.ListTablesInput {}
	if tables, err := io.db.ListTables(params); err != nil {
		log.Println(err)
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
		log.Printf("DB DescribeTable ERROR: %s \n", err)
		if DEBUG_MODE_LOG { log.Println(err.Error()) }
		return resp, err
	}

	if DEBUG_MODE_LOG { log.Println(resp) }
	return resp, nil
}

func (io *Ddbio)DeleteTable(tableName string) error {
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
		log.Printf("Expect a status to be %s, but timed out", status)
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

func (ddbio *Ddbio)GetTableStatus(tableName string) (*string, error) {
	desc, err := ddbio.DescribeTable(tableName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return desc.Table.TableStatus, nil
}
