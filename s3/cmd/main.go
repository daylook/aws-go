package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type BucketBasics struct {
	S3Client *s3.Client
}

func NewS3Client(sdkConfig aws.Config) *s3.Client {
	return s3.NewFromConfig(sdkConfig)
}

// This example uses the default settings specified in your shared credentials
// and config files.
func main() {
	ctx := context.Background()

	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}

	s3Client := s3.NewFromConfig(sdkConfig)

	s3App := &BucketBasics{
		S3Client: s3Client,
	}

	buckets, err := s3App.ListBucketsSimple(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	if buckets != nil {
		fmt.Println("Your buckets:")
		for _, b := range buckets {
			fmt.Printf("* %s (created on %v)\n", aws.ToString(b.Name), aws.ToTime(b.CreationDate))
		}
	}
}

func (basics *BucketBasics) ListBucketsSimple(ctx context.Context) ([]types.Bucket, error) {

	result, err := basics.S3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "AccessDenied" {
			return nil, fmt.Errorf("don't have permission to list buckets")
		} else {
			return nil, fmt.Errorf("couldn't list buckets: %v", err)
		}
	}

	if len(result.Buckets) == 0 {
		log.Println("You don't have any buckets!")
		return nil, nil
	}
	return result.Buckets, nil
}
