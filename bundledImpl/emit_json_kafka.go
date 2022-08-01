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
	"bytes"
	"context"
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"io"
	"log"
	"os"
	"text/template"
)

type KafkaEmitEvent struct {
	Data     map[string]interface{}
	Tmpl     *template.Template
	Producer *kafka.Writer
	Topic    string
	ctx      context.Context
}

func (kee *KafkaEmitEvent) Process(reader io.Reader, customParams interface{}) bool {

	fmt.Println("Kafka Emit process")

	fe := customParams.(*api.FileEntity)

	kee.Data["name"] = fe.Name
	kee.Data["filesize"] = fe.Size

	buf := &bytes.Buffer{}
	err := kee.Tmpl.Execute(buf, kee.Data)
	if err != nil {
		panic(err)
	}
	err = kee.Producer.WriteMessages(kee.ctx,
		kafka.Message{
			Key:   []byte(""),
			Value: buf.Bytes()})

	if err != nil {
		return false
	}
	return true
}

func (kee *KafkaEmitEvent) Setup(customParams interface{}) bool {
	fmt.Println("Kafka Emit setup")

	b, err := os.ReadFile(viper.GetString("kafkaemit.template"))
	if err != nil {
		panic(err)
	}

	jsonTemplate := string(b)

	kee.Data = make(map[string]interface{}, 8)
	kee.Tmpl = template.Must(template.New("kafkaemit.tmpl").Parse(jsonTemplate))

	kee.Data["traceid"] = "123"
	kee.Data["createtime"] = "123"
	kee.Data["linecount"] = "FilMeIn"
	kee.Data["modifiedtime"] = "123"
	kee.Data["name"] = "FilMeIn"
	kee.Data["path"] = "FilMeIn"
	kee.Data["qualifiedname"] = "FillMeIn"
	kee.Data["sum"] = "FillMeIn"

	logger := log.New(os.Stdout, "kafka producer: ", 0)
	kee.Producer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{viper.GetString("kafkaemit.bootstrap")},
		Topic:   "kafkaemit.topic",
		Logger:  logger,
	})

	kee.ctx = context.Background()

	// Does a sanity check to get an early fail on faulty templates
	err = kee.Tmpl.Execute(&bytes.Buffer{}, kee.Data)
	if err != nil {
		panic(err) //
	}

	return true
}
