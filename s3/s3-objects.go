package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ListObjects lists the objects in a bucket.
func (basics BucketBasics) ListObjects(ctx context.Context, bucketName string) ([]types.Object, error) {
	var err error
	var output *s3.ListObjectsV2Output
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	var objects []types.Object
	objectPaginator := s3.NewListObjectsV2Paginator(basics.S3Client, input)
	for objectPaginator.HasMorePages() {
		output, err = objectPaginator.NextPage(ctx)
		if err != nil {
			var noBucket *types.NoSuchBucket
			if errors.As(err, &noBucket) {
				log.Printf("Bucket %s does not exist.\n", bucketName)
				err = noBucket
			}
			break
		} else {
			objects = append(objects, output.Contents...)
		}
	}
	return objects, err
}

// ListObjectVersions lists all versions of all objects in a bucket.
func (basics BucketBasics) ListObjectVersions(ctx context.Context, bucket string) ([]types.ObjectVersion, error) {
	var err error
	var output *s3.ListObjectVersionsOutput
	var versions []types.ObjectVersion
	input := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
	versionPaginator := s3.NewListObjectVersionsPaginator(basics.S3Client, input)
	for versionPaginator.HasMorePages() {
		output, err = versionPaginator.NextPage(ctx)
		if err != nil {
			var noBucket *types.NoSuchBucket
			if errors.As(err, &noBucket) {
				log.Printf("Bucket %s does not exist.\n", bucket)
				err = noBucket
			}
			break
		} else {
			versions = append(versions, output.Versions...)
		}
	}
	return versions, err
}

// DeleteObjects deletes a list of objects from a bucket.
func (basics BucketBasics) DeleteObjects(ctx context.Context, bucket string, objects []types.ObjectIdentifier, bypassGovernance bool) error {
	if len(objects) == 0 {
		return nil
	}

	input := s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}
	if bypassGovernance {
		input.BypassGovernanceRetention = aws.Bool(true)
	}
	delOut, err := basics.S3Client.DeleteObjects(ctx, &input)
	if err != nil || len(delOut.Errors) > 0 {
		log.Printf("Error deleting objects from bucket %s.\n", bucket)
		if err != nil {
			var noBucket *types.NoSuchBucket
			if errors.As(err, &noBucket) {
				log.Printf("Bucket %s does not exist.\n", bucket)
				err = noBucket
			}
		} else if len(delOut.Errors) > 0 {
			for _, outErr := range delOut.Errors {
				log.Printf("%s: %s\n", *outErr.Key, *outErr.Message)
			}
			err = fmt.Errorf("%s", *delOut.Errors[0].Message)
		}
	} else {
		for _, delObjs := range delOut.Deleted {
			err = s3.NewObjectNotExistsWaiter(basics.S3Client).Wait(
				ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: delObjs.Key}, time.Minute)
			if err != nil {
				log.Printf("Failed attempt to wait for object %s to be deleted.\n", *delObjs.Key)
			} else {
				log.Printf("Deleted %s.\n", *delObjs.Key)
			}
		}
	}
	return err
}

// DeleteObject deletes an object from a bucket.
func (basics BucketBasics) DeleteObject(ctx context.Context, bucket string, key string, versionId string, bypassGovernance bool) (bool, error) {
	deleted := false
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if versionId != "" {
		input.VersionId = aws.String(versionId)
	}
	if bypassGovernance {
		input.BypassGovernanceRetention = aws.Bool(true)
	}
	_, err := basics.S3Client.DeleteObject(ctx, input)
	if err != nil {
		var noKey *types.NoSuchKey
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &noKey) {
			log.Printf("Object %s does not exist in %s.\n", key, bucket)
			err = noKey
		} else if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "AccessDenied":
				log.Printf("Access denied: cannot delete object %s from %s.\n", key, bucket)
				err = nil
			case "InvalidArgument":
				if bypassGovernance {
					log.Printf("You cannot specify bypass governance on a bucket without lock enabled.")
					err = nil
				}
			}
		}
	} else {
		err = s3.NewObjectNotExistsWaiter(basics.S3Client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s in bucket %s to be deleted.\n", key, bucket)
		} else {
			deleted = true
		}
	}
	return deleted, err
}

// DownloadLargeObject uses a download manager to download an object from a bucket.
// The download manager gets the data in parts and writes them to a buffer until all of
// the data has been downloaded.
func (basics BucketBasics) DownloadLargeObject(ctx context.Context, bucketName string, objectKey string) ([]byte, error) {
	var partMiBs int64 = 10
	downloader := manager.NewDownloader(basics.S3Client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't download large object from %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
	}
	return buffer.Bytes(), err
}

// UploadLargeObject uses an upload manager to upload data to an object in a bucket.
// The upload manager breaks large data into parts and uploads the parts concurrently.
func (basics BucketBasics) UploadLargeObject(ctx context.Context, bucketName string, objectKey string, largeObject []byte) error {
	largeBuffer := bytes.NewReader(largeObject)
	var partMiBs int64 = 10
	uploader := manager.NewUploader(basics.S3Client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * 1024 * 1024
	})
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   largeBuffer,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Printf("Error while uploading object to %s. The object is too large.\n"+
				"The maximum size for a multipart upload is 5TB.", bucketName)
		} else {
			log.Printf("Couldn't upload large object to %v:%v. Here's why: %v\n",
				bucketName, objectKey, err)
		}
	} else {
		err = s3.NewObjectExistsWaiter(basics.S3Client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", objectKey)
		}
	}

	return err
}

// GetObjectLegalHold retrieves the legal hold status for an S3 object.
func (basics BucketBasics) GetObjectLegalHold(ctx context.Context, bucket string, key string, versionId string) (*types.ObjectLockLegalHoldStatus, error) {
	var status *types.ObjectLockLegalHoldStatus
	input := &s3.GetObjectLegalHoldInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionId),
	}

	output, err := basics.S3Client.GetObjectLegalHold(ctx, input)
	if err != nil {
		var noSuchKeyErr *types.NoSuchKey
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &noSuchKeyErr) {
			log.Printf("Object %s does not exist in bucket %s.\n", key, bucket)
			err = noSuchKeyErr
		} else if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchObjectLockConfiguration":
				log.Printf("Object %s does not have an object lock configuration.\n", key)
				err = nil
			case "InvalidRequest":
				log.Printf("Bucket %s does not have an object lock configuration.\n", bucket)
				err = nil
			}
		}
	} else {
		status = &output.LegalHold.Status
	}

	return status, err
}

// GetObjectLockConfiguration retrieves the object lock configuration for an S3 bucket.
func (basics BucketBasics) GetObjectLockConfiguration(ctx context.Context, bucket string) (*types.ObjectLockConfiguration, error) {
	var lockConfig *types.ObjectLockConfiguration
	input := &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucket),
	}

	output, err := basics.S3Client.GetObjectLockConfiguration(ctx, input)
	if err != nil {
		var noBucket *types.NoSuchBucket
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &noBucket) {
			log.Printf("Bucket %s does not exist.\n", bucket)
			err = noBucket
		} else if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ObjectLockConfigurationNotFoundError" {
			log.Printf("Bucket %s does not have an object lock configuration.\n", bucket)
			err = nil
		}
	} else {
		lockConfig = output.ObjectLockConfiguration
	}

	return lockConfig, err
}

// GetObjectRetention retrieves the object retention configuration for an S3 object.
func (basics BucketBasics) GetObjectRetention(ctx context.Context, bucket string, key string) (*types.ObjectLockRetention, error) {
	var retention *types.ObjectLockRetention
	input := &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := basics.S3Client.GetObjectRetention(ctx, input)
	if err != nil {
		var noKey *types.NoSuchKey
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &noKey) {
			log.Printf("Object %s does not exist in bucket %s.\n", key, bucket)
			err = noKey
		} else if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchObjectLockConfiguration":
				err = nil
			case "InvalidRequest":
				log.Printf("Bucket %s does not have locking enabled.", bucket)
				err = nil
			}
		}
	} else {
		retention = output.Retention
	}

	return retention, err
}

// UploadObject uses the S3 upload manager to upload an object to a bucket.
func (basics BucketBasics) UploadObject(ctx context.Context, bucket string, key string, contents string) (string, error) {
	var outKey string
	input := &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              bytes.NewReader([]byte(contents)),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}
	output, err := basics.S3Manager.Upload(ctx, input)
	if err != nil {
		var noBucket *types.NoSuchBucket
		if errors.As(err, &noBucket) {
			log.Printf("Bucket %s does not exist.\n", bucket)
			err = noBucket
		}
	} else {
		err := s3.NewObjectExistsWaiter(basics.S3Client).Wait(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist in %s.\n", key, bucket)
		} else {
			outKey = *output.Key
		}
	}
	return outKey, err
}

// PutObjectLegalHold sets the legal hold configuration for an S3 object.
func (basics BucketBasics) PutObjectLegalHold(ctx context.Context, bucket string, key string, versionId string, legalHoldStatus types.ObjectLockLegalHoldStatus) error {
	input := &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		LegalHold: &types.ObjectLockLegalHold{
			Status: legalHoldStatus,
		},
	}
	if versionId != "" {
		input.VersionId = aws.String(versionId)
	}

	_, err := basics.S3Client.PutObjectLegalHold(ctx, input)
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Object %s does not exist in bucket %s.\n", key, bucket)
			err = noKey
		}
	}

	return err
}

// EnableObjectLockOnBucket enables object locking on an existing bucket.
func (basics BucketBasics) EnableObjectLockOnBucket(ctx context.Context, bucket string) error {
	// Versioning must be enabled on the bucket before object locking is enabled.
	verInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			MFADelete: types.MFADeleteDisabled,
			Status:    types.BucketVersioningStatusEnabled,
		},
	}
	_, err := basics.S3Client.PutBucketVersioning(ctx, verInput)
	if err != nil {
		var noBucket *types.NoSuchBucket
		if errors.As(err, &noBucket) {
			log.Printf("Bucket %s does not exist.\n", bucket)
			err = noBucket
		}
		return err
	}

	input := &s3.PutObjectLockConfigurationInput{
		Bucket: aws.String(bucket),
		ObjectLockConfiguration: &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
		},
	}
	_, err = basics.S3Client.PutObjectLockConfiguration(ctx, input)
	if err != nil {
		var noBucket *types.NoSuchBucket
		if errors.As(err, &noBucket) {
			log.Printf("Bucket %s does not exist.\n", bucket)
			err = noBucket
		}
	}

	return err
}

// PutObjectRetention sets the object retention configuration for an S3 object.
func (basics BucketBasics) PutObjectRetention(ctx context.Context, bucket string, key string, retentionMode types.ObjectLockRetentionMode, retentionPeriodDays int32) error {
	input := &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            retentionMode,
			RetainUntilDate: aws.Time(time.Now().AddDate(0, 0, int(retentionPeriodDays))),
		},
		BypassGovernanceRetention: aws.Bool(true),
	}

	_, err := basics.S3Client.PutObjectRetention(ctx, input)
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Object %s does not exist in bucket %s.\n", key, bucket)
			err = noKey
		}
	}

	return err
}
