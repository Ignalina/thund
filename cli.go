package main

import (
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/ignalina/thund/bundledImpl"
	"github.com/spf13/viper"
	"os"
)

func main() {
	viper.AddConfigPath(os.Args[1])
	viper.SetConfigName("config")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	fa := api.Processor{
		WatcherImpl: bundledImpl.NewS3(),
		IOEventImpl: []api.IOEvent{bundledImpl.DummyEvent{}, bundledImpl.KafkaEmitEvent{}},
	}

	fa.Start()
	// 		IOEventImpl: []api.IOEvent{bundledImpl.DummyEvent{}, bundledImpl.KafkaEmitEvent{}},
}
