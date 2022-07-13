package api

import "io"

type IOEvent interface {
	Setup() bool
	Process(reader io.Reader, customParams interface{}) bool
}
