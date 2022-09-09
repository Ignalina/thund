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
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/ignalina/thund/api"
	"github.com/spf13/viper"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type HDFS struct {
	Namenode      string
	WatchFolder   string
	MarkerFolder  string
	ExcludeFolder string
	GraceMilliSec int
	HDFSClient    *hdfs.Client
	Ctx           context.Context
	User          string
}

// Process all files , and for every sucessful process , add a ".done" file.

func (hdfsStruct *HDFS) Watch(eventHandlers []api.IOEvent) (bool, error) {
	ctx := context.Background()
	hdfsStruct.initHDFS()
	hdfsStruct.Ctx = ctx
	setupOk := true

	for index, handler := range eventHandlers {
		setupOk = handler.Setup(hdfsStruct)
		if !setupOk {
			log.Fatalln("Failed setup eventHandler nr " + strconv.Itoa(index))
			return false, nil
		}
	}

	for true {

		fileEntityMap, err := hdfsStruct.ListFiles()
		if nil != err {
			return false, err
		}

		for _, fileEntity := range fileEntityMap {

			//	setupOk := event.Setup()
			//			o, _ := hdfsStruct.HDFSClient.ReadFile(fileEntity.Name)
			var sucess bool = true
			o, err := hdfsStruct.HDFSClient.Open(fileEntity.Name)
			if err != nil {
				return false, err
			}

			defer o.Close()
			//			return ioutil.ReadAll(o)

			for _, handler := range eventHandlers {
				sucess := handler.Process(o, &fileEntity)
				if !sucess {
					break
				}

			}

			if sucess { // create a marker file with original file name + ".done"
				dummyFile := "Dummy file"
				dummyFileName := fileEntity.Name + ".done"

				//dummyFileName=bundledImpl.FullDestinPath(hdfsStruct.MarkerFolder, fullpath, extension)
				dummyFileName=path.Join(hdfsStruct.MarkerFolder, path.Base(fileEntity.Name))+".done"
				log.Printf("planned Markerfilename="+dummyFileName)
				writer, err := hdfsStruct.HDFSClient.Create(dummyFileName)
				if nil != err {
					return false, err
				}

				_, err = writer.Write([]byte(dummyFile))

				if nil != err {
					return false, err
				}

			}

		}

		time.Sleep(time.Duration(hdfsStruct.GraceMilliSec) * time.Millisecond)
	}

	return true, nil
}

func NewHDFS() *HDFS {
	hdfs := HDFS{
		Namenode:      viper.GetString("hdfs.namenode"),
		User:          viper.GetString("hdfs.user"),
		WatchFolder:   viper.GetString("hdfs.watchfolder"),
		MarkerFolder:  viper.GetString("hdfs.markerfolder"),
		ExcludeFolder: viper.GetString("hdfs.excludefolder"),
		GraceMilliSec: viper.GetInt("hdfs.gracemillisec"),
	}
	return &hdfs
}

func (hdfsStruct HDFS) ListFiles() (map[string]api.FileEntity, error) {

	watchDir, err := hdfsStruct.HDFSClient.ReadDir(hdfsStruct.WatchFolder)
	if nil != err {
		return nil, err
	}

	markerDir, err := hdfsStruct.HDFSClient.ReadDir(hdfsStruct.MarkerFolder)
	if nil != err {
		return nil, err
	}

	res_files := make(map[string]api.FileEntity)

	for _, object := range watchDir {

		if object.IsDir() || strings.HasPrefix(object.Name(), hdfsStruct.ExcludeFolder) {
			continue
		}

		res_files[object.Name()] = api.FileEntity{
			Name: path.Join(hdfsStruct.WatchFolder, object.Name()),
			Size: object.Size(),
		}
	}

	marker_files := make(map[string]api.FileEntity)
	for _, object := range markerDir {

		if object.IsDir() || strings.HasPrefix(object.Name(), hdfsStruct.ExcludeFolder) || !strings.HasSuffix(object.Name(), ".done") {
			continue
		}

		marker_files[object.Name()] = api.FileEntity{
			Name: path.Join(hdfsStruct.WatchFolder, object.Name()),
			Size: object.Size(),
		}
	}

	// remove all files that are done OR inside a exclude folder.
	for k, _ := range res_files {

		if _, exists := marker_files[k+".done"]; exists {
			delete(res_files, k)
			delete(res_files, k+".done")
		}

	}
	return res_files, nil
}

func (hdfsStruct *HDFS) initHDFS() *hdfs.Client {
	//	ctx := context.Background()
	var err error
	hdfsStruct.HDFSClient, err = GetClient(hdfsStruct.Namenode, hdfsStruct.User)

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", hdfsStruct.HDFSClient) // hdfsClient is now set up
	return hdfsStruct.HDFSClient
}

func (hdfsStruct HDFS) GetFileSet() map[string]int64 {
	return hdfsStruct.GetFilteredFileSet("")
}

func (hdfsStruct HDFS) GetFilteredFileSet(filter string) map[string]int64 {
	m := make(map[string]int64)

	entries, err := hdfsStruct.ListFiles()

	if err != nil {
		return nil
	}
	for _, ent := range entries {
		m[ent.Name] = int64(ent.Size)
	}

	return m
}

var cachedClients map[string]*hdfs.Client = make(map[string]*hdfs.Client)

// Code copied from colinmarc/hdfs
func GetClient(namenode string, user string) (*hdfs.Client, error) {
	if cachedClients[namenode] != nil {
		return cachedClients[namenode], nil
	}

	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("Problem loading configuration: %s", err)
	}

	options := hdfs.ClientOptionsFromConf(conf)
	if namenode != "" {
		options.Addresses = strings.Split(namenode, ",")
	}

	if options.Addresses == nil {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment.")
	}

	if options.KerberosClient != nil {
		panic("NOT IMPLEMENTED")
		//		options.KerberosClient, err = getKerberosClient()
		if err != nil {
			return nil, fmt.Errorf("Problem with kerberos authentication: %s", err)
		}
	} else {
		options.User = user
	}

	// Set some basic defaults.
	dialFunc := (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 5 * time.Second,
		DualStack: true,
	}).DialContext

	options.NamenodeDialFunc = dialFunc
	options.DatanodeDialFunc = dialFunc

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("Couldn't connect to namenode: %s", err)
	}

	cachedClients[namenode] = c
	return c, nil
}
