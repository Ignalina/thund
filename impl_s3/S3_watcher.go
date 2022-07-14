package impl_s3

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
	MinioClient   *minio.Client
	Ctx           context.Context

	//	Frontagent api.FrontAgent
}

type FileEntity struct {
	Name string
	Size int64
}

// Process all files , and for every sucessful process , add a ".done" file.

func (s3 S3) Watch(event api.IOEvent) bool {
	s3.Ctx = context.Background()
	s3.MinioClient = s3.initS3()

	setupOk := event.Setup(s3)

	for setupOk {

		files := s3.ListFiles(s3.MinioClient)
		for _, v := range files {

			opts := minio.GetObjectOptions{}
			o, _ := s3.MinioClient.GetObject(s3.Ctx, s3.BucketName, v.Name, opts)

			sucess := event.Process(o, v)
			if sucess { // create a marker file with original file name + ".done"
				dummyFile := "Dummy file"
				dummyFileName := v.Name + ".done"

				myReader := strings.NewReader(dummyFileName)
				s3.MinioClient.PutObject(s3.Ctx, s3.BucketName, dummyFileName, myReader, int64(len(dummyFile)), minio.PutObjectOptions{ContentType: "application/text"})
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
	//	ctx := context.Background()
	minioClient, err := minio.New(s3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.User, s3.Password, s3.Token),
		Secure: s3.UseSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", minioClient) // minioClient is now set up
	return minioClient
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
