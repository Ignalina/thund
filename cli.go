package main

import (
	"github.com/ignalina/thund/api"
	"github.com/ignalina/thund/impl"
)


func main() {

	fa := api.FrontAgent{
		IOEventImpl: impl.DummyEvent{},
		WatcherImpl: impl.S3{
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
