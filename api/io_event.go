package api

import "io"

type IOEvent interface {
	Process(reader io.Reader, customParams interface{}) bool
}
