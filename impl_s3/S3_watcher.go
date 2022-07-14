/*
 * MIT No Attribution
 *
 * Copyright 2022 Rickard Ernst Björn Lundin (rickard@ignalina.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package impl_s3

import (
	"context"
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"log"
	"strconv"
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
}

// Process all files , and for every sucessful process , add a ".done" file.

func (s3 S3) Watch(eventHandlers []api.IOEvent) bool {
	ctx := context.Background()
	minioClient := s3.initS3()
	minioClient.IsOnline()
	setupOk := true

	for index, handler := range eventHandlers {
		setupOk = handler.Setup(s3)
		if setupOk {
			log.Fatalln("Failed setup eventHandler nr " + strconv.Itoa(index))
		}
	}

	for setupOk {

		files := s3.ListFiles(s3.MinioClient)
		for _, v := range files {

			opts := minio.GetObjectOptions{}

			//	setupOk := event.Setup()
			o, _ := minioClient.GetObject(ctx, s3.BucketName, v.Name, opts)
			var sucess bool = true

			for _, handler := range eventHandlers {
				sucess := handler.Process(o, v)
				if !sucess {
					break
				}

			}

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

func NewS3() S3 {
	s3 := S3{
		Endpoint:      viper.GetString("s3.endpoint"), // 10.1.1.22
		UseSSL:        viper.GetBool("s3.usessl"),
		BucketName:    viper.GetString("s3.bucketname"), //"bucket1"
		User:          viper.GetString("s3.user"),
		Password:      viper.GetString("s3.password"),
		Token:         viper.GetString("s3.token"),
		WatchFolder:   viper.GetString("s3.watchfolder"),
		GraceMilliSec: viper.GetInt("s3.gracemillisec"),
	}
	return s3
}

func (s3 S3) ListFiles(minioClient *minio.Client) map[string]api.FileEntity {

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	objectCh := minioClient.ListObjects(ctx, s3.BucketName, minio.ListObjectsOptions{
		WithVersions: false,
		WithMetadata: true,
		Recursive:    false,
		//		Prefix:       s3.WatchFolder,
	})

	res_files := make(map[string]api.FileEntity)

	for object := range objectCh {
		if object.Err != nil {
			fmt.Println(object.Err)
			return nil
		}

		if !strings.HasSuffix(object.Key, "/") {
			res_files[object.Key] = api.FileEntity{
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
