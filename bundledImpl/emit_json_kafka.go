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
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ignalina/thund/api"
	"github.com/spf13/viper"
	"io"
	"os"
	"text/template"
)

type KafkaEmitEvent struct {
	Data     map[string]interface{}
	Tmpl     *template.Template
	Producer *kafka.Producer
	Topic    string
}

func (kee *KafkaEmitEvent) Process(reader io.Reader, customParams interface{}) bool {

	fmt.Println("Kafka Emit process")

	//	var fe *api.FileEntity
	fe := customParams.(*api.FileEntity)

	kee.Data["name"] = fe.Name
	kee.Data["filesize"] = fe.Size

	buf := &bytes.Buffer{}
	err := kee.Tmpl.Execute(buf, kee.Data)
	if err != nil {
		panic(err)
	}
	s := buf.String()
	fmt.Println("Kafka Emit about to send json="+s)
		
	err =kee.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kee.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(s),
	}, nil)

	if err != nil {
		fmt.Println("Could not send to kafka")
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

	kee.Producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": viper.GetString("kafkaemit.bootstrap")})
	if err != nil {
		panic(err)
	}

	//	defer kee.Producer.Close()

	kee.Topic = viper.GetString("kafkaemit.topic")

	return true
}
