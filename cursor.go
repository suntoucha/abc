package abc

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Cursor struct {
	s              *s3.S3
	bucket, prefix string
	output         *s3.ListObjectsV2Output
	index          int
}

func (c *Cursor) Init(s *s3.S3, bucket string, prefix string) {
	c.s = s
	c.bucket = bucket
	c.prefix = prefix
	c.output = nil
	c.index = 0
}

func (c *Cursor) Next() (string, bool, error) {
	var err error

	if c.output != nil && len(c.output.Contents) == 0 {
		return "", false, nil
	}

	if c.output == nil || c.index == len(c.output.Contents) {
		params := s3.ListObjectsV2Input{
			Bucket:  aws.String(c.bucket),
			MaxKeys: aws.Int64(1000),
		}

		if c.prefix != "" {
			params.Prefix = aws.String(c.prefix)
		}

		if c.output != nil {
			tmp := *c.output.Contents[len(c.output.Contents)-1].Key
			params.StartAfter = aws.String(tmp)
		}

		if c.output, err = c.s.ListObjectsV2(&params); err != nil {
			return "", false, err
		}

		if len(c.output.Contents) == 0 {
			return "", false, nil
		}

		c.index = 0
	}

	tmp := *c.output.Contents[c.index].Key
	c.index++

	return tmp, true, nil
}
