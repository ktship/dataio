package dataio

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
)

var config = &aws.Config {
	Endpoint: aws.String(URL_dynamoDB),
	Region: aws.String("us-west-2"),
	MaxRetries:              aws.Int(2),
}

func CreateUserTable() error {
	svc := dynamodb.New(config)
	// JSON 파라메터
	// 기본키 : uid (string)
	// 쓰루풋 : 인 1, 아웃 1
	params := &dynamodb.CreateTableInput {
		TableName: aws.String("users"),
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
	resp, err := svc.CreateTable(params)
	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return err
	}
	if DEBUG_MODE { fmt.Println(resp) }
	return nil
}

func DescribeTable(tableName string) error {
	svc := dynamodb.New(config)

	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	resp, err := svc.DescribeTable(params)

	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Println(resp)
	return nil
}

func DeleteTable(tableName string) error {
	svc := dynamodb.New(config)

	params := &dynamodb.DeleteTableInput{
		TableName: aws.String("users"),
	}
	resp, err := svc.DeleteTable(params)

	if err != nil {
		if DEBUG_MODE { fmt.Println(err.Error()) }
		return err
	}

	// Pretty-print the response data.
	if DEBUG_MODE { fmt.Println(resp) }
	return nil
}

func UpdateUser() {
	svc := dynamodb.New(config)

	params := &dynamodb.GetItemInput{
		TableName: aws.String("TableName"),

		AttributesToGet: []*string{
			aws.String("AttributeName"), // Required
			// More values...
		},

		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S:    aws.String("StringAttributeValue"),
			},
		},
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"Key": aws.String("AttributeName"),
		},
		ProjectionExpression:   aws.String("ProjectionExpression"),
		ReturnConsumedCapacity: aws.String("ReturnConsumedCapacity"),
	}
	resp, err := svc.GetItem(params)

	if err != nil {
		// Print the error, cast err to awserr. Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	fmt.Println(resp)


	result, err := svc.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("Tables:")
	for _, table := range result.TableNames {
		log.Println(*table)
	}

}
