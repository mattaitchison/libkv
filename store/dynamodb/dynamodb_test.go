package dynamodb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	region          = "us-east-1"
	endpoint        = "http://localhost:8000"
	accessKeyID     = "abc"
	secretAccessKey = "abc"
)

func TestCreateTable(t *testing.T) {
	// if endpoint == "" {
	// 	t.SkipNow()
	// }
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			SessionToken:    "",
		}},
		&credentials.EnvProvider{},
	})
	awsConf := aws.NewConfig().
		WithCredentials(creds).
		WithRegion(region).
		WithEndpoint(endpoint)
	client := dynamodb.New(session.New(awsConf))
	var randInt = rand.New(rand.NewSource(time.Now().UnixNano())).Int()
	table := fmt.Sprintf("dynamodb-test-%d", randInt)

	err := ensureTableExists(client, table, DefaultDynamoDBReadCapacity, DefaultDynamoDBWriteCapacity)
	assert.NoError(t, err)

	defer func() {
		client.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
	}()
}

func TestRegister(t *testing.T) {
	Register()

	var randInt = rand.New(rand.NewSource(time.Now().UnixNano())).Int()
	table := fmt.Sprintf("dynamodb-test-%d", randInt)
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			SessionToken:    "",
		}},
		&credentials.EnvProvider{},
	})
	awsConf := aws.NewConfig().
		WithCredentials(creds).
		WithRegion(region).
		WithEndpoint(endpoint)
	client := dynamodb.New(session.New(awsConf))
	os.Setenv("AWS_DYNAMODB_TABLE", table)
	os.Setenv("AWS_REGION", region)
	os.Setenv("AWS_ACCESS_KEY_ID", accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", secretAccessKey)
	defer func() {
		client.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
		os.Unsetenv("AWS_DYNAMODB_TABLE")
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		os.Unsetenv("AWS_REGION")
	}()

	kv, err := libkv.NewStore(
		store.DYNAMODB,
		[]string{endpoint},
		nil,
	)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing dynamodb")
	}

}

func TestAll(t *testing.T) {
	Register()

	var randInt = rand.New(rand.NewSource(time.Now().UnixNano())).Int()
	table := fmt.Sprintf("dynamodb-test-%d", randInt)
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			SessionToken:    "",
		}},
		&credentials.EnvProvider{},
	})
	awsConf := aws.NewConfig().
		WithCredentials(creds).
		WithRegion(region).
		WithEndpoint(endpoint)
	client := dynamodb.New(session.New(awsConf))
	os.Setenv("AWS_DYNAMODB_TABLE", table)
	os.Setenv("AWS_REGION", region)
	os.Setenv("AWS_ACCESS_KEY_ID", accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", secretAccessKey)
	defer func() {
		client.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
		os.Unsetenv("AWS_DYNAMODB_TABLE")
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		os.Unsetenv("AWS_REGION")
	}()

	kv, err := libkv.NewStore(
		store.DYNAMODB,
		[]string{endpoint},
		nil,
	)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing dynamodb")
	}

	key1 := "libabc/kvaabc"
	value1 := []byte("testval123")
	err = kv.Put(key1, value1, nil)
	assert.NoError(t, err)

	pair1, err := kv.Get(key1)
	assert.NoError(t, err)
	if assert.NotNil(t, pair1) {
		assert.NotNil(t, pair1.Value)
	}
	assert.Equal(t, pair1.Key, key1)
	assert.Equal(t, pair1.Value, value1)

	// AtomicPut using kv1 and kv2 should succeed
	_, _, err = kv.AtomicPut(key1, []byte("TestnewVal1"), pair1, nil)
	assert.NoError(t, err)

	// AtomicPut using kv1 and kv2 should fail
	_, _, err = kv.AtomicPut(key1, []byte("TestnewVal2"), pair1, nil)
	assert.Error(t, err)

	testutils.RunTestCommon(t, kv)

	err = kv.Delete(key1)
	assert.NoError(t, err)

}
