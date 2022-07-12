package impl

import (
	"fmt"
	"io"
)

type DummyEvent struct {
	//	Frontagent api.FrontAgent
}

func (de DummyEvent) Process(reader io.Reader, customParams interface{}) bool {
	//	s3.Frontagent.File_event_impl.Process(nil, nil)
	//	event.Process(nil, nil, "ahah")
	fmt.Println("dummy")
	return true
}
