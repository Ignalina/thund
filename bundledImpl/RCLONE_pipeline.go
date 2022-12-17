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
	_ "github.com/rclone/rclone/backend/all"
	_ "github.com/rclone/rclone/backend/drive"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/sync"
	"github.com/spf13/viper"
	"log"
	"time"
)

type RCLONE struct {
	ViperKey        string
	GraceMilliSec   int
	CollectStat     bool
	EntriesBefore   int
	EntriesAfter    int
	SourcePath      string
	DestinationPath string

	//	HDFSClient    *hdfs.Client
	Ctx context.Context
	//	User          string
}

// Process all files , and for every sucessful process , add a ".done" file.
func (rcloneStruct *RCLONE) Setup(customParams interface{}) error {
	ctx := context.Background()
	//	rcloneStruct.initHDFS()
	rcloneStruct.Ctx = ctx
	//	setupOk := true

	log.Println("RCLONE nodeprocessor  v2022-12-11 sucessfully set up")
	return nil
}

func (rcloneStruct *RCLONE) Process() error {
	rcloneStruct.Ctx = rcloneStruct.Ctx

	configfile.Install()

	fsource, err := fs.NewFs(rcloneStruct.Ctx, rcloneStruct.SourcePath)
	if err != nil {
		log.Fatal(err)
	}

	fdestination, err := fs.NewFs(rcloneStruct.Ctx, rcloneStruct.DestinationPath)
	if err != nil {
		log.Fatal(err)
	}

	if rcloneStruct.CollectStat {
		entriesBefore, err := fsource.List(context.Background(), "/")
		if err != nil {
			rcloneStruct.EntriesBefore = 0
			time.Sleep(time.Duration(rcloneStruct.GraceMilliSec) * time.Millisecond * 10)
			return err
		} else {
			rcloneStruct.EntriesBefore = entriesBefore.Len()
		}
	}

	// TODO perhpahs configurable operation instead of hardcoded CopyDir
	sync.CopyDir(rcloneStruct.Ctx, fdestination, fsource, false)

	// TODO should only wait if GraceMillisec period has passed.
	time.Sleep(time.Duration(rcloneStruct.GraceMilliSec) * time.Millisecond * 10)

	if rcloneStruct.CollectStat {
		entriesAfter, err := fdestination.List(context.Background(), "/")
		if err != nil {
			rcloneStruct.EntriesBefore = 0
			rcloneStruct.EntriesAfter = 0
			time.Sleep(time.Duration(rcloneStruct.GraceMilliSec) * time.Millisecond * 10)
			return err
		} else {
			rcloneStruct.EntriesAfter = entriesAfter.Len()
		}
	}

	return nil
}

func NewRCLONE(viperKey string) *RCLONE {
	rclone := RCLONE{
		ViperKey:        viperKey,
		GraceMilliSec:   viper.Sub(viperKey).GetInt("rclone.gracemillisec"),
		SourcePath:      viper.Sub(viperKey).GetString("rclone.sourcepath"),
		DestinationPath: viper.Sub(viperKey).GetString("rclone.destinationpath"),
		CollectStat:     viper.Sub(viperKey).GetBool("rclone.collectstat"),
	}
	return &rclone
}
