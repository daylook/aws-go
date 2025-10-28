package s3

import (
	"context"
	"crypto/rand"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/demotools"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/s3/actions"
)

// RunLargeObjectScenario is an interactive example that shows you how to use Amazon
// Simple Storage Service (Amazon S3) to upload and download large objects.
//
// 1. Create a bucket.
// 3. Upload a large object to the bucket by using an upload manager.
// 5. Download a large object by using a download manager.
// 8. Delete all objects in the bucket.
// 9. Delete the bucket.
//
// This example creates an Amazon S3 service client from the specified sdkConfig so that
// you can replace it with a mocked or stubbed config for unit testing.
//
// It uses a questioner from the `demotools` package to get input during the example.
// This package can be found in the ..\..\demotools folder of this repo.
func RunLargeObjectScenario(ctx context.Context, sdkConfig aws.Config, questioner demotools.IQuestioner) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Something went wrong with the demo.")
			_, isMock := questioner.(*demotools.MockQuestioner)
			if isMock || questioner.AskBool("Do you want to see the full error message (y/n)?", "y") {
				log.Println(r)
			}
		}
	}()

	log.Println(strings.Repeat("-", 88))
	log.Println("Welcome to the Amazon S3 large object demo.")
	log.Println(strings.Repeat("-", 88))

	s3Client := s3.NewFromConfig(sdkConfig)
	bucketBasics := actions.BucketBasics{S3Client: s3Client}

	bucketName := questioner.Ask("Let's create a bucket. Enter a name for your bucket:", demotools.NotEmpty{})

	bucketExists, err := bucketBasics.BucketExists(ctx, bucketName)
	if err != nil {
		panic(err)
	}

	if !bucketExists {
		err = bucketBasics.CreateBucket(ctx, bucketName, sdkConfig.Region)
		if err != nil {
			panic(err)
		} else {
			log.Println("Bucket created.")
		}
	}

	log.Println(strings.Repeat("-", 88))

	mibs := 30
	log.Printf("Let's create a slice of %v MiB of random bytes and upload it to your bucket. ", mibs)

	questioner.Ask("Press Enter when you're ready.")

	largeBytes := make([]byte, 1024*1024*mibs)
	_, _ = rand.Read(largeBytes)
	largeKey := "doc-example-large"
	log.Println("Uploading...")

	err = bucketBasics.UploadLargeObject(ctx, bucketName, largeKey, largeBytes)
	if err != nil {
		panic(err)
	}

	log.Printf("Uploaded %v MiB object as %v", mibs, largeKey)
	log.Println(strings.Repeat("-", 88))
	log.Printf("Let's download the %v MiB object.", mibs)

	questioner.Ask("Press Enter when you're ready.")
	log.Println("Downloading...")

	largeDownload, err := bucketBasics.DownloadLargeObject(ctx, bucketName, largeKey)
	if err != nil {
		panic(err)
	}

	log.Printf("Downloaded %v bytes.", len(largeDownload))
	log.Println(strings.Repeat("-", 88))

	if questioner.AskBool("Do you want to delete your bucket and all of its "+
		"contents? (y/n)", "y") {
		log.Println("Deleting object.")
		err = bucketBasics.DeleteObjects(ctx, bucketName, []string{largeKey})
		if err != nil {
			panic(err)
		}
		log.Println("Deleting bucket.")
		err = bucketBasics.DeleteBucket(ctx, bucketName)
		if err != nil {
			panic(err)
		}
	} else {
		log.Println("Okay. Don't forget to delete objects from your bucket to avoid charges.")
	}
	log.Println(strings.Repeat("-", 88))

	log.Println("Thanks for watching!")
	log.Println(strings.Repeat("-", 88))
}

// DemoBucket contains metadata for buckets used in this example.
type DemoBucket struct {
	name             string
	retentionEnabled bool
	// objectKeys       []string
}

// Resources keeps track of AWS resources created during the ObjectLockScenario and handles
// cleanup when the scenario finishes.
type Resources struct {
	basics      *BucketBasics
	demoBuckets map[string]*DemoBucket
	questioner  demotools.IQuestioner
}

// init initializes objects in the Resources struct.
// func (resources *Resources) init(basics *BucketBasics, questioner demotools.IQuestioner) {
// 	resources.basics = basics
// 	resources.questioner = questioner
// 	resources.demoBuckets = map[string]*DemoBucket{}
// }

// Cleanup deletes all AWS resources created during the ObjectLockScenario.
func (resources *Resources) Cleanup(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Something went wrong during cleanup.\n%v\n", r)
			log.Println("Use the AWS Management Console to remove any remaining resources " +
				"that were created for this scenario.")
		}
	}()

	wantDelete := resources.questioner.AskBool("Do you want to remove all of the AWS resources that were created "+
		"during this demo (y/n)?", "y")
	if !wantDelete {
		log.Println("Be sure to remove resources when you're done with them to avoid unexpected charges!")
		return
	}

	log.Println("Removing objects from S3 buckets and deleting buckets...")
	resources.deleteBuckets(ctx, resources.demoBuckets)
	//resources.deleteRetentionObjects(resources.retentionBucket, resources.retentionObjects)

	log.Println("Cleanup complete.")
}

// deleteBuckets empties and then deletes all buckets created during the ObjectLockScenario.
func (resources *Resources) deleteBuckets(ctx context.Context, createInfo map[string]*DemoBucket) {
	for _, info := range createInfo {
		bucket := resources.demoBuckets[info.name]
		resources.deleteObjects(ctx, bucket)
		_, err := resources.basics.S3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucket.name),
		})
		if err != nil {
			panic(err)
		}
	}

	for _, info := range createInfo {
		bucket := resources.demoBuckets[info.name]
		err := s3.NewBucketNotExistsWaiter(resources.basics.S3Client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket.name)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to be deleted.\n", bucket.name)
		} else {
			log.Printf("Deleted %s.\n", bucket.name)
		}
	}
	resources.demoBuckets = map[string]*DemoBucket{}
}

// deleteObjects deletes all objects in the specified bucket.
func (resources *Resources) deleteObjects(ctx context.Context, bucket *DemoBucket) {
	lockConfig, err := resources.basics.GetObjectLockConfiguration(ctx, bucket.name)
	if err != nil {
		panic(err)
	}
	versions, err := resources.basics.ListObjectVersions(ctx, bucket.name)
	if err != nil {
		switch err.(type) {
		case *types.NoSuchBucket:
			log.Printf("No objects to get from %s.\n", bucket.name)
		default:
			panic(err)
		}
	}
	delObjects := make([]types.ObjectIdentifier, len(versions))
	for i, version := range versions {
		if lockConfig != nil && lockConfig.ObjectLockEnabled == types.ObjectLockEnabledEnabled {
			status, err := resources.basics.GetObjectLegalHold(ctx, bucket.name, *version.Key, *version.VersionId)
			if err != nil {
				switch err.(type) {
				case *types.NoSuchKey:
					log.Printf("Couldn't determine legal hold status for %s in %s.\n", *version.Key, bucket.name)
				default:
					panic(err)
				}
			} else if status != nil && *status == types.ObjectLockLegalHoldStatusOn {
				err = resources.basics.PutObjectLegalHold(ctx, bucket.name, *version.Key, *version.VersionId, types.ObjectLockLegalHoldStatusOff)
				if err != nil {
					switch err.(type) {
					case *types.NoSuchKey:
						log.Printf("Couldn't turn off legal hold for %s in %s.\n", *version.Key, bucket.name)
					default:
						panic(err)
					}
				}
			}
		}
		delObjects[i] = types.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId}
	}
	err = resources.basics.DeleteObjects(ctx, bucket.name, delObjects, bucket.retentionEnabled)
	if err != nil {
		switch err.(type) {
		case *types.NoSuchBucket:
			log.Println("Nothing to delete.")
		default:
			panic(err)
		}
	}
}
