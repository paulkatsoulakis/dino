package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3 is an implementation of Store backed by AWS S3.
type S3 struct {
	profile string
	region  string
	bucket  string
	client  *s3.S3
}

func NewS3(profile, region, bucket string) Store {
	return &S3{
		profile: profile,
		region:  region,
		bucket:  bucket,
	}
}

func (s *S3) Get(key []byte) (value []byte, err error) {
	if err := s.ensureClient(); err != nil {
		return nil, err
	}
	hexKey := fmt.Sprintf("%x", key)
	output, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hexKey),
	})
	if err != nil {
		if rfErr, ok := err.(awserr.RequestFailure); ok {
			if rfErr.StatusCode() == http.StatusNotFound {
				return nil, fmt.Errorf("%q: %w", key, ErrNotFound)
			}
		}
		return nil, err
	}
	defer func() {
		if err := output.Body.Close(); err != nil {
			log.WithFields(log.Fields{
				"op":  "get",
				"key": hexKey,
			}).Warning("Could not close response body")
		}
	}()
	return ioutil.ReadAll(output.Body)
}

func (s *S3) Put(key, value []byte) (err error) {
	err = s.ensureClient()
	if err == nil {
		hexKey := fmt.Sprintf("%x", key)
		_, err = s.client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(hexKey),
			Body:   bytes.NewReader(value),
		})
	}
	return
}

func (s *S3) ensureClient() error {
	if s.client != nil {
		return nil
	}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.region),
		Credentials: credentials.NewSharedCredentials("", s.profile),
	})
	if err != nil {
		return err
	}
	client := s3.New(sess)
	s.client = client
	return nil
}
