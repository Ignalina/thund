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
	"context"
	"github.com/spf13/viper"
	"log"
)

type RCLONE struct {
	GraceMilliSec int
	//	HDFSClient    *hdfs.Client
	Ctx context.Context
	//	User          string
}

// Process all files , and for every sucessful process , add a ".done" file.
func (rcloneStruct *RCLONE) Setup() error {
	ctx := context.Background()
	//	rcloneStruct.initHDFS()
	rcloneStruct.Ctx = ctx
	//	setupOk := true

	log.Println("RCLONE nodeprocessor  v2022-12-11 sucessfully set up")
	return nil
}

func (rcloneStruct *RCLONE) Process() error {
	ctx := context.Background()
	rcloneStruct.Ctx = ctx

	// TODO call RCLONE API
	return nil
}

func NewRCLONE() *RCLONE {
	rclone := RCLONE{
		GraceMilliSec: viper.GetInt("hdfs.gracemillisec"),
	}
	return &rclone
}
