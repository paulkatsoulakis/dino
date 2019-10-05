package storage

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/nicolagi/dino/message"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type DynamoDBVersionedStore struct {
	profile string
	region  string
	table   string
	opts    options
	local   VersionedStore

	// Do throttling on our side based on configured RCUs/WCUs so the
	// client doesn't have to retry.
	getLimiter *rate.Limiter
	putLimiter *rate.Limiter

	ddb        *dynamodb.DynamoDB
	ddbstreams *dynamodbstreams.DynamoDBStreams
}

func NewDynamoDBVersionedStore(profile, region, table string, opts ...Option) (*DynamoDBVersionedStore, error) {
	s := &DynamoDBVersionedStore{
		profile: profile,
		region:  region,
		table:   table,
		local:   NewVersionedWrapper(NewInMemoryStore()),
	}
	for _, o := range opts {
		o(&s.opts)
	}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.region),
		Credentials: credentials.NewSharedCredentials("", s.profile),
	})
	if err != nil {
		return nil, err
	}
	s.ddb = dynamodb.New(sess)
	s.ddbstreams = dynamodbstreams.New(sess)
	if err := s.configureLimiters(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *DynamoDBVersionedStore) configureLimiters() error {
	result, err := s.ddb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &s.table,
	})
	if err != nil {
		return err
	}
	// Assume our items, that we get/put individually, are <= 1 kB,
	// so that RCUs/WCUs translate to get/put requests per second.
	// It's a fair assumption, in fact, our items are much smaller.
	rcus := *result.Table.ProvisionedThroughput.ReadCapacityUnits
	wcus := *result.Table.ProvisionedThroughput.WriteCapacityUnits
	s.getLimiter = rate.NewLimiter(rate.Every(time.Duration(1_000_000/rcus)*time.Microsecond), 1)
	s.putLimiter = rate.NewLimiter(rate.Every(time.Duration(1_000_000/wcus)*time.Microsecond), 1)
	return nil
}

func (s *DynamoDBVersionedStore) Put(version uint64, key []byte, value []byte) (err error) {
	k := ddbBinary(key)
	va := ddbBinary(value)
	ve := ddbNumber(version)
	var input dynamodb.PutItemInput
	input.TableName = &s.table
	input.ConditionExpression = aws.String("attribute_not_exists(ve) or (ve < :ourVersion)")
	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":ourVersion": ve,
	}
	input.Item = map[string]*dynamodb.AttributeValue{
		"k":  k,
		"ve": ve,
		"va": va,
	}
	time.Sleep(s.putLimiter.Reserve().Delay())
	_, err = s.ddb.PutItem(&input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return ErrStalePut
			}
		}
		return err
	}
	putMessage := message.NewPutMessage(0, string(key), string(value), version)
	if response := ApplyMessage(s.local, putMessage); response.Kind() == message.KindError {
		log.WithFields(log.Fields{
			"err": response.Value(),
		}).Error("Could not apply locally our own successful put")
	}
	return nil
}

func (s *DynamoDBVersionedStore) Get(key []byte) (version uint64, value []byte, err error) {
	version, value, err = s.local.Get(key)
	if err == nil {
		log.WithFields(log.Fields{
			"key":     fmt.Sprintf("%.10x", key),
			"version": version,
		}).Debug("Returning local version")
		return
	}
	var input dynamodb.GetItemInput
	input.TableName = &s.table
	input.Key = map[string]*dynamodb.AttributeValue{
		"k": ddbBinary(key),
	}
	time.Sleep(s.getLimiter.Reserve().Delay())
	output, err := s.ddb.GetItem(&input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return 0, nil, fmt.Errorf("%v: %w", e, ErrNotFound)
			}
		}
		return 0, nil, err
	}
	if output.Item == nil {
		return 0, nil, fmt.Errorf("%.10x: %w", key, ErrNotFound)
	}
	value = output.Item["va"].B
	// Trusting this to be a number.
	version, _ = strconv.ParseUint(*output.Item["ve"].N, 10, 64)
	return version, value, nil
}

func ddbBinary(b []byte) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		B: dup(b),
	}
}

func ddbNumber(n uint64) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		N: aws.String(strconv.FormatUint(n, 10)),
	}
}
