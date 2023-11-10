package abc

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"strings"
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
	b, err := a.GetRaw(bucket, key)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, x)
}

func (a *ABC) GetRaw(bucket, key string) ([]byte, error) {
	input := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	obj, err := a.s.GetObject(&input)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(obj.Body)
}

func (a *ABC) Put(bucket string, key string, x interface{}) error {
	tmp, err := json.Marshal(x)
	if err != nil {
		return err
	}

	return a.PutRaw(bucket, key, tmp)
}

func (a *ABC) PutRaw(bucket string, key string, b []byte) error {
	object := s3.PutObjectInput{
		Bucket: aws.String(bucket),        // The path to the directory you want to upload the object to, starting with your Space name.
		Key:    aws.String(key),           // Object key, referenced whenever you want to access this file later.
		Body:   bytes.NewReader(b),        // The object's contents.
		ACL:    aws.String("public-read"), // Defines Access-control List (ACL) permissions, such as private or public.
	}

	_, err := a.s.PutObject(&object)
	if err != nil {
		return err
	}

	return nil
}

func (a *ABC) List(bucket string, prefix string) ([]string, error) {
	params := s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(1000),
	}
	if prefix != "" {
		params.Prefix = aws.String(prefix)
	}

	list := []*s3.ListObjectsV2Output{}
	for {
		output, err := a.s.ListObjectsV2(&params)
		if err != nil {
			return nil, err
		}
		list = append(list, output)

		if len(output.Contents) == 0 {
			break
		} else {
			params.StartAfter = output.Contents[len(output.Contents)-1].Key
		}
	}

	str := []string{}
	for _, x := range list {
		for _, o := range x.Contents {
			tmp := *o.Key
			str = append(str, tmp)
		}
	}

	return str, nil
}

func (a *ABC) Exists(bucket, key string) (bool, error) {
	params := s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := a.s.HeadObject(&params)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (a *ABC) Cursor(bucket, prefix string) Cursor {
	var cur Cursor
	cur.Init(a.s, bucket, prefix)

	return cur
}
