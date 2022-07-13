package impl

import (
	"context"
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"log"
	"strings"
	"time"
)

type S3 struct {
	Endpoint      string
	UseSSL        bool
	BucketName    string
	User          string
	Password      string
	Token         string
	WatchFolder   string
	GraceMilliSec int
	//	Frontagent api.FrontAgent
}

type FileEntity struct {
	Name string
	Size int64
}

// Process all files , and for every sucessful process , add a ".done" file.

func (s3 S3) Watch(event api.IOEvent) bool {
	ctx := context.Background()
	minioClient := s3.initS3()
	minioClient.IsOnline()
	setupOk := event.Setup()

	for setupOk {

		files := s3.ListFiles(minioClient)
		for _, v := range files {

			opts := minio.GetObjectOptions{}
			o, _ := minioClient.GetObject(ctx, s3.BucketName, v.Name, opts)

			sucess := event.Process(o, v)
			if sucess { // create a marker file with original file name + ".done"
				dummyFile := "Dummy file"
				dummyFileName := v.Name + ".done"

				myReader := strings.NewReader(dummyFileName)
				minioClient.PutObject(ctx, s3.BucketName, dummyFileName, myReader, int64(len(dummyFile)), minio.PutObjectOptions{ContentType: "application/text"})
			}
		}

		time.Sleep(time.Duration(s3.GraceMilliSec) * time.Millisecond)
	}

	return true
}

func (s3 S3) ListFiles(minioClient *minio.Client) map[string]FileEntity {

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	objectCh := minioClient.ListObjects(ctx, s3.BucketName, minio.ListObjectsOptions{
		WithVersions: false,
		WithMetadata: true,
		Recursive:    false,
		//		Prefix:       s3.WatchFolder,
	})

	res_files := make(map[string]FileEntity)

	for object := range objectCh {
		if object.Err != nil {
			fmt.Println(object.Err)
			return nil
		}

		if !strings.HasSuffix(object.Key, "/") {
			res_files[object.Key] = FileEntity{
				Name: object.Key,
				Size: object.Size,
			}
		}
	}

	// remove all files that are done.
	for k, v := range res_files {
		if strings.HasSuffix(v.Name, ".done") {
			delete(res_files, k)
		} else if _, exists := res_files[k+".done"]; exists {
			delete(res_files, k)
		}
	}
	return res_files
}

func (s3 S3) initS3() *minio.Client {
	ctx := context.Background()
	minioClient, err := minio.New(s3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.User, s3.Password, s3.Token),
		Secure: s3.UseSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", minioClient) // minioClient is now set up
	err = minioClient.MakeBucket(ctx, s3.BucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, s3.BucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", s3.BucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", s3.BucketName)
	}
	return minioClient
}

func (s3 S3) GetReadyFileSet(minioclient *minio.Client) map[string]int64 {

	ls1 := s3.GetFileSet(minioclient)
	GraceMilliSec := s3.GraceMilliSec
	time.Sleep(time.Duration(GraceMilliSec) * time.Millisecond)
	ls2 := s3.GetFileSet(minioclient)
	for fileName, _ := range ls1 {
		if _, exists := ls2[fileName]; exists { // dummy test to check that file still exists
			sizeDiff := ls1[fileName] - ls2[fileName] //Check that file size is consistent
			if sizeDiff != 0 {
				delete(ls1, fileName) //   Remove file since size differs (Might be growing still)
			}
		}
	}

	return ls1
}

func (s3 S3) GetFileSet(minioclient *minio.Client) map[string]int64 {
	return s3.GetFilteredFileSet("", minioclient)
}

func (s3 S3) GetFilteredFileSet(filter string, minioClient *minio.Client) map[string]int64 {
	m := make(map[string]int64)
	entries := s3.ListFiles(minioClient)

	for _, ent := range entries {
		m[ent.Name] = int64(ent.Size)
	}

	return m
}
