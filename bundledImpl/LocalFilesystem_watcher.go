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
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type LocalFilesystem struct {
	WatchFolder   string
	ExcludeFolder string
	GraceMilliSec int
	Ctx           context.Context
}

// Process all files , and for every sucessful process , add a ".done" file.

func (lf *LocalFilesystem) Watch(eventHandlers []api.IOEvent) (bool, error) {
	ctx := context.Background()
	lf.Ctx = ctx
	setupOk := true

	for index, handler := range eventHandlers {
		setupOk = handler.Setup(lf)
		if !setupOk {
			log.Fatalln("Failed setup eventHandler nr " + strconv.Itoa(index))
		}
	}

	for setupOk {

		fileEntityMap := lf.ListFiles()
		for _, fileEntity := range fileEntityMap {

			//	setupOk := event.Setup()
			o, err := os.Open(fileEntity.Name)
			if err != nil {
				fmt.Println(err)
				continue
			}
			defer o.Close()
			var sucess bool = true

			for _, handler := range eventHandlers {
				sucess := handler.Process(o, &fileEntity)
				if !sucess {
					break
				}

			}

			if sucess { // create a marker file with original file name + ".done"
				dummyFile := "Dummy file"
				dummyFileName := fileEntity.Name + ".done"
				err = os.WriteFile(dummyFileName, []byte(dummyFile), 0644)
			}

		}

		time.Sleep(time.Duration(lf.GraceMilliSec) * time.Millisecond)
	}

	return true,nil
}

func NewLocalFilesystem() *LocalFilesystem {
	lf := LocalFilesystem{
		WatchFolder:   viper.GetString("LocalFilesystem.watchfolder"),
		ExcludeFolder: viper.GetString("LocalFilesystem.excludefolder"),
		GraceMilliSec: viper.GetInt("LocalFilesystem.gracemillisec"),
	}
	return &lf
}

func (lf *LocalFilesystem) ListFiles() map[string]api.FileEntity {

	files, err := ioutil.ReadDir(lf.WatchFolder)
	if err != nil {
		log.Fatal(err)
	}

	res_files := make(map[string]api.FileEntity)

	for _, file := range files {

		if !file.IsDir() && !strings.HasPrefix(file.Name(), lf.ExcludeFolder) {
			res_files[file.Name()] = api.FileEntity{
				Name: file.Name(),
				Size: file.Size(),
			}
		}
	}

	// remove all files that are done OR inside a exclude folder.
	for k, _ := range res_files {
		if _, exists := res_files[k+".done"]; exists {
			delete(res_files, k)
			delete(res_files, k+".done")
		}
	}
	return res_files
}
