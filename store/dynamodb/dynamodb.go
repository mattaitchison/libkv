package dynamodb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when multiple endpoints specified for
	// BoltDB. Endpoint has to be a local file path
	ErrMultipleEndpointsUnsupported = errors.New("dynamodb does not support multiple endpoints")

	ErrBucketOptionMissing = errors.New("Bucket config option missing")
)

const (
	// DefaultDynamoDBRegion is used when no region is configured
	// explicitly.
	DefaultDynamoDBRegion = "us-east-1"

	// DefaultDynamoDBReadCapacity is the default read capacity
	// that is used when none is configured explicitly.
	DefaultDynamoDBReadCapacity = 5
	// DefaultDynamoDBWriteCapacity is the default write capacity
	// that is used when none is configured explicitly.
	DefaultDynamoDBWriteCapacity = 5

	// DynamoDBEmptyPath is the string that is used instead of
	// empty strings when stored in DynamoDB.
	DynamoDBEmptyPath = " "

	// DynamoDBLockPrefix is the prefix used to mark DynamoDB records
	// as locks. This prefix causes them not to be returned by
	// List operations.
	DynamoDBLockPrefix = "_"
)

// DynamoDBRecord is the representation of a vault entry in
// DynamoDB. The vault key is split up into two components
// (Path and Key) in order to allow more efficient listings.
type DynamoDBRecord struct {
	Path  string
	Key   string
	Value []byte
}

// DynamoDB store
type DynamoDB struct {
	table  string
	client *dynamodb.DynamoDB
}

// Register registers dynamodb to libkv
func Register() {
	libkv.AddStore(store.DYNAMODB, New)
}

// New creates a DynamoDB store
func New(endpoints []string, options *store.Config) (store.Store, error) {
	var (
		client *dynamodb.DynamoDB
		table  string
	)

	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	if (options == nil) || (len(options.Bucket) == 0) {
		return nil, ErrBucketOptionMissing
	}

	table = options.Bucket
	client = dynamodb.New(session.New())

	readCapacityString := os.Getenv("AWS_DYNAMODB_READ_CAPACITY")
	if readCapacityString == "" {
		readCapacityString = "0"
	}
	readCapacity, err := strconv.Atoi(readCapacityString)
	if err != nil {
		return nil, fmt.Errorf("invalid read capacity: %s", readCapacityString)
	}
	if readCapacity == 0 {
		readCapacity = DefaultDynamoDBReadCapacity
	}

	writeCapacityString := os.Getenv("AWS_DYNAMODB_WRITE_CAPACITY")
	if writeCapacityString == "" {
		writeCapacityString = "0"
	}
	writeCapacity, err := strconv.Atoi(writeCapacityString)
	if err != nil {
		return nil, fmt.Errorf("invalid write capacity: %s", writeCapacityString)
	}
	if writeCapacity == 0 {
		writeCapacity = DefaultDynamoDBWriteCapacity
	}

	if err := ensureTableExists(client, table, readCapacity, writeCapacity); err != nil {
		return nil, err
	}

	d := &DynamoDB{
		table:  table,
		client: client,
	}
	return d, nil
}

// Put mock
func (d *DynamoDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	defer metrics.MeasureSince([]string{"dynamodb", "put"}, time.Now())
	record := DynamoDBRecord{
		Path:  recordPathForVaultKey(key),
		Key:   recordKeyForVaultKey(key),
		Value: value,
	}

	requests := []*dynamodb.WriteRequest{{
		PutRequest: &dynamodb.PutRequest{
			Item: map[string]*dynamodb.AttributeValue{
				"Path":  {S: aws.String(record.Path)},
				"Key":   {S: aws.String(record.Key)},
				"Value": {B: record.Value},
			},
		},
	}}

	for _, prefix := range prefixes(key) {
		record = DynamoDBRecord{
			Path: recordPathForVaultKey(prefix),
			Key:  fmt.Sprintf("%s/", recordKeyForVaultKey(prefix)),
		}
		item, err := dynamodbattribute.ConvertToMap(record)
		if err != nil {
			return fmt.Errorf("could not convert prefix record to DynamoDB item: %s", err)
		}
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})
	}
	return d.batchWriteRequests(requests)
}

// Get mock
func (d *DynamoDB) Get(key string) (*store.KVPair, error) {
	defer metrics.MeasureSince([]string{"dynamodb", "get"}, time.Now())

	// d.permitPool.Acquire()
	// defer d.permitPool.Release()

	resp, err := d.client.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(d.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Path": {S: aws.String(recordPathForVaultKey(key))},
			"Key":  {S: aws.String(recordKeyForVaultKey(key))},
		},
	})
	if err != nil {
		return nil, err
	}
	if resp.Item == nil {
		return nil, store.ErrKeyNotFound
	}

	record := &DynamoDBRecord{}
	if err := dynamodbattribute.ConvertFromMap(resp.Item, record); err != nil {
		return nil, err
	}

	return &store.KVPair{
		Key:   vaultKey(record),
		Value: record.Value,
	}, nil
}

// Delete mock
func (d *DynamoDB) Delete(key string) error {
	defer metrics.MeasureSince([]string{"dynamodb", "delete"}, time.Now())

	requests := []*dynamodb.WriteRequest{{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: map[string]*dynamodb.AttributeValue{
				"Path": {S: aws.String(recordPathForVaultKey(key))},
				"Key":  {S: aws.String(recordKeyForVaultKey(key))},
			},
		},
	}}

	// clean up now empty 'folders'
	prefixes := prefixes(key)
	sort.Sort(sort.Reverse(sort.StringSlice(prefixes)))
	for _, prefix := range prefixes {
		items, err := d.List(prefix)
		if err != nil {
			return err
		}
		if len(items) == 1 {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"Path": {S: aws.String(recordPathForVaultKey(prefix))},
						"Key":  {S: aws.String(fmt.Sprintf("%s/", recordKeyForVaultKey(prefix)))},
					},
				},
			})
		}
	}

	return d.batchWriteRequests(requests)
}

// Exists mock
func (d *DynamoDB) Exists(key string) (bool, error) {

	defer metrics.MeasureSince([]string{"dynamodb", "exists"}, time.Now())

	// d.permitPool.Acquire()
	// defer d.permitPool.Release()

	resp, err := d.client.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(d.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Path": {S: aws.String(recordPathForVaultKey(key))},
			"Key":  {S: aws.String(recordKeyForVaultKey(key))},
		},
	})
	if err != nil {
		return false, err
	}
	if resp.Item == nil {
		return false, nil
	}

	if len(resp.Item["Value"].B) == 0 {
		return false, store.ErrKeyNotFound
	}
	return true, nil

}

// Watch mock
func (d *DynamoDB) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree mock
func (d *DynamoDB) WatchTree(prefix string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// NewLock mock
func (d *DynamoDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// List mock
func (d *DynamoDB) List(prefix string) ([]*store.KVPair, error) {
	defer metrics.MeasureSince([]string{"dynamodb", "list"}, time.Now())

	prefix = strings.TrimSuffix(prefix, "/")

	prefix = escapeEmptyPath(prefix)
	queryInput := &dynamodb.QueryInput{
		TableName:      aws.String(d.table),
		ConsistentRead: aws.Bool(true),
		KeyConditions: map[string]*dynamodb.Condition{
			"Path": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{{
					S: aws.String(prefix),
				}},
			},
		},
	}

	// d.permitPool.Acquire()
	// defer d.permitPool.Release()

	kv := []*store.KVPair{}

	err := d.client.QueryPages(queryInput, func(out *dynamodb.QueryOutput, lastPage bool) bool {
		var record DynamoDBRecord
		for _, item := range out.Items {
			dynamodbattribute.ConvertFromMap(item, &record)
			if !strings.HasPrefix(record.Key, DynamoDBLockPrefix) {
				kv = append(kv, &store.KVPair{
					Key:   record.Key,
					Value: record.Value,
					// LastIndex: dbIndex,
				})
			}
		}
		return !lastPage
	})
	if err != nil {
		return nil, err
	}

	if len(kv) == 0 {
		return nil, store.ErrKeyNotFound
	}

	return kv, nil
}

// DeleteTree mock
func (d *DynamoDB) DeleteTree(prefix string) error {
	defer metrics.MeasureSince([]string{"dynamodb", "deletetree"}, time.Now())

	requests := []*dynamodb.WriteRequest{{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: map[string]*dynamodb.AttributeValue{
				"Path": {S: aws.String(recordPathForVaultKey(prefix))},
				"Key":  {S: aws.String(recordKeyForVaultKey(prefix))},
			},
		},
	}}

	items, err := d.List(prefix)
	if err != nil {
		return err
	}

	for _, item := range items {
		if !strings.HasPrefix(item.Key, DynamoDBLockPrefix) {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						"Path": {S: aws.String(prefix)},
						"Key":  {S: aws.String(recordKeyForVaultKey(item.Key))},
					},
				},
			})
		}
	}

	return d.batchWriteRequests(requests)
}

// AtomicPut mock
func (d *DynamoDB) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	defer metrics.MeasureSince([]string{"dynamodb", "aomicput"}, time.Now())
	var prev []byte
	if previous != nil {
		prev = previous.Value
	}

	params := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(recordKeyForVaultKey(key)),
			},
			"Path": {
				S: aws.String(recordPathForVaultKey(key)),
			},
		},
		TableName:                aws.String(d.table),
		UpdateExpression:         aws.String("SET #value = :newval"),
		ConditionExpression:      aws.String("#value = :prev"),
		ExpressionAttributeNames: aws.StringMap(map[string]string{"#value": "Value"}),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":newval": {B: value},
			":prev":   {B: prev},
		},
		ReturnValues: aws.String("ALL_NEW"),
	}
	resp, err := d.client.UpdateItem(params)
	if err != nil {
		return false, nil, err
	}

	record := &DynamoDBRecord{}
	if err := dynamodbattribute.ConvertFromMap(resp.Attributes, record); err != nil {
		return true, nil, err
	}

	updated := &store.KVPair{
		Key:   key,
		Value: record.Value,
		// LastIndex: dbIndex,
	}
	return true, updated, nil
}

// AtomicDelete mock
func (d *DynamoDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return false, store.ErrCallNotSupported
}

// Lock mock
func (d *DynamoDB) Lock(stopCh chan struct{}) (<-chan struct{}, error) {
	return nil, store.ErrCallNotSupported
}

// Close mock
func (d *DynamoDB) Close() {
	return
}

// batchWriteRequests takes a list of write requests and executes them in badges
// with a maximum size of 25 (which is the limit of BatchWriteItem requests).
func (d *DynamoDB) batchWriteRequests(requests []*dynamodb.WriteRequest) error {

	for len(requests) > 0 {
		batchSize := int(math.Min(float64(len(requests)), 25))
		batch := requests[:batchSize]
		requests = requests[batchSize:]

		// s.permitPool.Acquire()
		_, err := d.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				d.table: batch,
			},
		})
		// s.permitPool.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// ensureTableExists creates a DynamoDB table with a given
// DynamoDB client. If the table already exists, it is not
// being reconfigured.
func ensureTableExists(client *dynamodb.DynamoDB, table string, readCapacity, writeCapacity int) error {
	_, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if awserr, ok := err.(awserr.Error); ok {
		if awserr.Code() == "ResourceNotFoundException" {
			_, err = client.CreateTable(&dynamodb.CreateTableInput{
				TableName: aws.String(table),
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(int64(readCapacity)),
					WriteCapacityUnits: aws.Int64(int64(writeCapacity)),
				},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: aws.String("Path"),
					KeyType:       aws.String("HASH"),
				}, {
					AttributeName: aws.String("Key"),
					KeyType:       aws.String("RANGE"),
				}},
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: aws.String("Path"),
					AttributeType: aws.String("S"),
				}, {
					AttributeName: aws.String("Key"),
					AttributeType: aws.String("S"),
				}},
			})
			if err != nil {
				return err
			}

			err = client.WaitUntilTableExists(&dynamodb.DescribeTableInput{
				TableName: aws.String(table),
			})
			if err != nil {
				return err
			}
		}
	}
	return err
}

// recordPathForVaultKey transforms a vault key into
// a value suitable for the `DynamoDBRecord`'s `Path`
// property. This path equals the the vault key without
// its last component.
func recordPathForVaultKey(key string) string {
	if strings.Contains(key, "/") {
		return filepath.Dir(key)
	}
	return DynamoDBEmptyPath
}

// recordKeyForVaultKey transforms a vault key into
// a value suitable for the `DynamoDBRecord`'s `Key`
// property. This path equals the the vault key's
// last component.
func recordKeyForVaultKey(key string) string {
	return filepath.Base(key)
}

// vaultKey returns the vault key for a given record
// from the DynamoDB table. This is the combination of
// the records Path and Key.
func vaultKey(record *DynamoDBRecord) string {
	path := unescapeEmptyPath(record.Path)
	if path == "" {
		return record.Key
	}
	return filepath.Join(record.Path, record.Key)
}

// escapeEmptyPath is used to escape the root key's path
// with a value that can be stored in DynamoDB. DynamoDB
// does not allow values to be empty strings.
func escapeEmptyPath(s string) string {
	if s == "" {
		return DynamoDBEmptyPath
	}
	return s
}

// unescapeEmptyPath is the opposite of `escapeEmptyPath`.
func unescapeEmptyPath(s string) string {
	if s == DynamoDBEmptyPath {
		return ""
	}
	return s
}

// prefixes returns all parent 'folders' for a given
// vault key.
// e.g. for 'foo/bar/baz', it returns ['foo', 'foo/bar']
func prefixes(s string) []string {
	components := strings.Split(s, "/")
	result := []string{}
	for i := 1; i < len(components); i++ {
		result = append(result, strings.Join(components[:i], "/"))
	}
	return result
}
