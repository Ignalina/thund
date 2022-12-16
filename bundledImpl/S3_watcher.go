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

package bundledImpl

import (
	"context"
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
	"log"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type S3 struct {
	ViperKey      string
	Endpoint      string
	UseSSL        bool
	BucketName    string
	User          string
	Password      string
	Token         string
	WatchFolder   string
	MarkerFolder  string
	ExcludeFolder string
	GraceMilliSec int
	MinioClient   *minio.Client
	Ctx           context.Context
	eventHandlers []api.IOEvent
}

func (s3 *S3) Setup(customParams interface{}) error {
	ctx := context.Background()
	s3.initS3()
	s3.Ctx = ctx
	s3.MinioClient.IsOnline()
	setupOk := true

	for index, handler := range s3.eventHandlers {
		setupOk = handler.Setup(s3)
		if !setupOk {
			log.Fatalln("Failed setup eventHandler nr " + strconv.Itoa(index))
			// todo return error
		}
	}
	return nil
}

// Process all files , and for every sucessful process , add a ".done" file.

func (s3 *S3) Process() error {
	ctx := context.Background()
	s3.initS3()
	s3.Ctx = ctx
	s3.MinioClient.IsOnline()

	fileEntityMap := s3.ListFiles(s3.MinioClient)
	keys := api.SortOnAge(fileEntityMap)
	for _, k := range keys {
		fileEntity := fileEntityMap[k]

		opts := minio.GetObjectOptions{}

		//	setupOk := event.Setup()
		o, _ := s3.MinioClient.GetObject(ctx, s3.BucketName, fileEntity.Name, opts)
		var sucess bool = true

		for _, handler := range s3.eventHandlers {
			sucess := handler.Process(o, &fileEntity)
			if !sucess {
				break
			}

		}

		// create a marker file with original file name + ".done" | ".igore"

		var dummyFileName string
		if sucess {
			dummyFileName = fileEntity.Name + ".done"
		} else {
			dummyFileName = fileEntity.Name + ".ignore"
		}

		dummyFile := "Dummy file"

		myReader := strings.NewReader(dummyFile)
		s3.MinioClient.PutObject(s3.Ctx, s3.BucketName, dummyFileName, myReader, int64(len(dummyFile)), minio.PutObjectOptions{ContentType: "application/text"})

	}

	time.Sleep(time.Duration(s3.GraceMilliSec) * time.Millisecond)

	return nil
}

func NewS3(viperKey string) *S3 {
	s3 := S3{
		ViperKey:      viperKey,
		Endpoint:      viper.Sub(viperKey).GetString("s3.endpoint"),
		UseSSL:        viper.Sub(viperKey).GetBool("s3.usessl"),
		BucketName:    viper.Sub(viperKey).GetString("s3.bucketname"),
		User:          viper.Sub(viperKey).GetString("s3.user"),
		Password:      viper.Sub(viperKey).GetString("s3.password"),
		Token:         viper.Sub(viperKey).GetString("s3.token"),
		WatchFolder:   viper.Sub(viperKey).GetString("s3.watchfolder"),
		MarkerFolder:  viper.Sub(viperKey).GetString("s3.markerfolder"),
		ExcludeFolder: viper.Sub(viperKey).GetString("s3.excludefolder"),
		GraceMilliSec: viper.Sub(viperKey).GetInt("s3.gracemillisec"),
	}
	return &s3
}

func (s3 S3) ListFiles(minioClient *minio.Client) map[string]api.FileEntity {

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	watchDir := minioClient.ListObjects(ctx, s3.BucketName, minio.ListObjectsOptions{
		WithVersions: false,
		WithMetadata: true,
		Recursive:    true,
		Prefix:       s3.WatchFolder,
	})

	markerDir := minioClient.ListObjects(ctx, s3.BucketName, minio.ListObjectsOptions{
		WithVersions: false,
		WithMetadata: true,
		Recursive:    true,
		Prefix:       s3.MarkerFolder,
	})

	res_files := make(map[string]api.FileEntity)

	for object := range watchDir {
		if object.Err != nil {
			fmt.Println(object.Err)
			return nil
		}

		if !strings.HasSuffix(object.Key, "/") && !strings.HasPrefix(object.Key, s3.ExcludeFolder) {
			res_files[object.Key] = api.FileEntity{
				Name: object.Key,
				Size: object.Size,
			}
		}
	}

	marker_files := make(map[string]api.FileEntity)

	for object := range markerDir {
		if object.Err != nil {
			fmt.Println(object.Err)
			return nil
		}

		if !strings.HasSuffix(object.Key, "/") && !strings.HasPrefix(object.Key, s3.ExcludeFolder) {
			marker_files[object.Key] = api.FileEntity{
				Name:      object.Key,
				UnixMilli: object.LastModified.UnixMilli(),
				Size:      object.Size,
			}
		}
	}

	// remove all files that are done OR inside a exclude folder.
	for k, _ := range res_files {
		if _, exists := marker_files[k+".done"]; exists {
			delete(res_files, k)
			delete(res_files, k+".done")
		}
		if _, exists := marker_files[k+".ignore"]; exists {
			delete(res_files, k)
			delete(res_files, k+".ignore")
		}

	}
	return res_files
}

func (s3 *S3) initS3() *minio.Client {
	//	ctx := context.Background()
	minioClient, err := minio.New(s3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.User, s3.Password, s3.Token),
		Secure: s3.UseSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	s3.MinioClient = minioClient

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

// extension example strconv.Itoa(key)+."parquet"
func FullDestinPath(destDir string, fullSourcePath string, extension string) string {
	destinName := path.Join(destDir, filepath.Base(fullSourcePath)+"_"+extension)
	return destinName
}
