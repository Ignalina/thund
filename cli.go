package main

import (
	"github.com/ignalina/thund/api"
	"github.com/ignalina/thund/bundledImpl"
	"github.com/ignalina/thund/impl_s3"
)

func main() {

	fa := api.Processor{
		WatcherImpl: impl_s3.NewS3(),
		IOEventImpl: []api.IOEvent{bundledImpl.DummyEvent{}, bundledImpl.KafkaEmitEvent{}},
	}

	fa.Start()

}
