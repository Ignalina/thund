package api

import "io"

type IOEvent interface {
	Setup(customParams interface{}) bool
	Process(reader io.Reader, customParams interface{}) bool
}
