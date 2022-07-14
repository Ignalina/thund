package main

import (
	"github.com/ignalina/thund/api"
	"github.com/ignalina/thund/impl_s3"
)

func main() {

	fa := api.FrontAgent{
		IOEventImpl: impl_s3.DummyEvent{},
		WatcherImpl: impl_s3.S3{
			Endpoint:      "10.1.1.22",
			UseSSL:        false,
			BucketName:    "bucket1",
			User:          "myuser",
			Password:      "mypassword",
			Token:         "",
			WatchFolder:   "",
			GraceMilliSec: 1000,
		},
	}

	fa.Start()

}
