package abc

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ABC struct {
	s *s3.S3
}

func (a *ABC) Init(key, secret, endpoint, region string) {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(key, secret, ""),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(false),
		Region:           aws.String(region),
	}

	newSession := session.New(s3Config)
	a.s = s3.New(newSession)
}

func (a *ABC) Get(bucket string, key string, x interface{}) error {
	return get(a.s, bucket, key, x)
}

func (a *ABC) Put(bucket string, key string, x interface{}) error {
	return put(a.s, bucket, key, x)
}

func get(s *s3.S3, bucket string, key string, x interface{}) error {
	input := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	obj, err := s.GetObject(&input)
	if err != nil {
		return err
	}

	return json.NewDecoder(obj.Body).Decode(x)
}

func put(s *s3.S3, bucket string, key string, x interface{}) error {
	tmp, err := json.Marshal(x)
	if err != nil {
		return err
	}

	object := s3.PutObjectInput{
		Bucket: aws.String(bucket),        // The path to the directory you want to upload the object to, starting with your Space name.
		Key:    aws.String(key),           // Object key, referenced whenever you want to access this file later.
		Body:   bytes.NewReader(tmp),      // The object's contents.
		ACL:    aws.String("public-read"), // Defines Access-control List (ACL) permissions, such as private or public.
	}

	_, err = s.PutObject(&object)
	if err != nil {
		return err
	}

	return nil
}
