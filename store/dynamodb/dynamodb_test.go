package dynamodb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

// func TestRegister(t *testing.T) {
// 	Register()
//
// 	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
// 		t.SkipNow()
// 	}
//
// 	_, err := credentials.NewEnvCredentials().Get()
// 	if err != nil {
// 		t.Fatalf("err: %v", err)
// 	}
//
// 	var randInt = rand.New(rand.NewSource(time.Now().UnixNano())).Int()
// 	table := fmt.Sprintf("boldDBTest-%d", randInt)
// 	conn := dynamodb.New(session.New())
// 	defer func() {
// 		conn.DeleteTable(&dynamodb.DeleteTableInput{
// 			TableName: aws.String(table),
// 		})
// 	}()
//
// 	kv, err := libkv.NewStore(
// 		store.DYNAMODB,
// 		[]string{},
// 		&store.Config{Bucket: table},
// 	)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, kv)
//
// 	if _, ok := kv.(*DynamoDB); !ok {
// 		t.Fatal("Error registering and initializing boltDB")
// 	}
//
// }
func TestAll(t *testing.T) {
	Register()

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.SkipNow()
	}

	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	var randInt = rand.New(rand.NewSource(time.Now().UnixNano())).Int()
	table := fmt.Sprintf("boldDBTest-%d", randInt)

	table = "boldDBTest-3254680447636280993"

	kv, err := libkv.NewStore(
		store.DYNAMODB,
		[]string{},
		&store.Config{Bucket: table},
	)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing boltDB")
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

	// err = kv.Delete(key1)
	// assert.NoError(t, err)
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
