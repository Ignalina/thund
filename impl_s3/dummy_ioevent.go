package impl_s3

import (
	"fmt"
	"io"
)

type DummyEvent struct {
}

func (de DummyEvent) Process(reader io.Reader, customParams interface{}) bool {
	fmt.Println("dummy process")
	return true
}

func (de DummyEvent) Setup() bool {
	fmt.Println("dummy setup")
	return true
}
