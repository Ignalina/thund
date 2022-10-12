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
	log.Println("HDFS watcher v2022-10-11 sucessfully set up all handlers")

	for true {

		fileEntityMap, err := hdfsStruct.FilterListFiles()
		if nil != err {
			log.Println("HDFS watcher could not list files due to " + err.Error())
			hdfsStruct.FindWorkingNamenode()
			time.Sleep(time.Duration(hdfsStruct.GraceMilliSec) * time.Millisecond * 10)
			continue
		}

		keys := api.SortOnAge(fileEntityMap)

		for _, k := range keys {
			fileEntity := fileEntityMap[k]
			var sucess bool = true
			o, err := hdfsStruct.HDFSClient.Open(fileEntity.Name)
			if err != nil {
				return false, err
			}

			defer o.Close()

			for index, handler := range eventHandlers {
				sucess = handler.Process(o, &fileEntity)
				if !sucess {
					log.Println("Handler " + strconv.Itoa(index) + " step return failure. Will not do more steps and place .ignore file")
					o.Close()
					break
				}
			}
			o.Close()

			// create a marker file with original file name + ".done" | ".igore"

			var dummyFileName string
			if (sucess) {
				dummyFileName = path.Join(hdfsStruct.MarkerFolder, path.Base(fileEntity.Name)) + ".done"
			} else {
				dummyFileName = path.Join(hdfsStruct.MarkerFolder, path.Base(fileEntity.Name)) + ".ignore"
			}

			dummyFile := "Dummy file"
			log.Printf("planned Markerfilename=" + dummyFileName)
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
			Name:      path.Join(hdfsStruct.WatchFolder, object.Name()),
			UnixMilli: object.ModTime().UnixMilli(),
			Size:      object.Size(),
		}
	}

	marker_files := make(map[string]api.FileEntity)
	for _, object := range markerDir {

		if object.IsDir() || strings.HasPrefix(object.Name(), hdfsStruct.ExcludeFolder) || !strings.HasSuffix(object.Name(), ".done") || !strings.HasSuffix(object.Name(), ".ignore") {
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

		if _, exists := marker_files[k+".ignore"]; exists {
			delete(res_files, k)
			delete(res_files, k+".ignore")
		}

	}
	return res_files, nil
}

func (hdfsStruct *HDFS) FindWorkingNamenode() {
	var err error

	nameNodes := strings.Split(hdfsStruct.Namenode, ",")
	fmt.Println(nameNodes)
	for _, nameNode := range nameNodes {
		hdfsStruct.HDFSClient, err = GetClient(nameNode, hdfsStruct.User)
		if err != nil {
			log.Println("Error getting hdfs client for namenode=" + nameNode + " err=" + err.Error())
			continue
		}

		fr, er := hdfsStruct.HDFSClient.Open(hdfsStruct.WatchFolder)
		if er != nil {
			log.Println("Could not access watchfolder for nameNode=" + nameNode + " err=" + er.Error())
			continue
		}
		fr.Close()
		break
	}
	// Todo very bad construct , might leave without working namenode

}

func (hdfsStruct *HDFS) initHDFS() *hdfs.Client {
	//	ctx := context.Background()

	log.Printf("Setting up hdfs client , looking for ", hdfsStruct.HDFSClient) // hdfsClient is now set up
	hdfsStruct.FindWorkingNamenode()
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

// TODO use https://github.com/deckarep/golang-set
func (hdfsStruct *HDFS) FilterListFiles() (map[string]api.FileEntity, error) {

	filteredFileEntityMap := make(map[string]api.FileEntity)

	prevFileEntityMap, err := hdfsStruct.ListFiles()
	if nil != err {
		log.Println("HDFS watcher could not list files due to " + err.Error())
		hdfsStruct.FindWorkingNamenode()
		time.Sleep(time.Duration(hdfsStruct.GraceMilliSec) * time.Millisecond * 10)
		return nil, err
	}

	time.Sleep(time.Duration(hdfsStruct.GraceMilliSec) * time.Millisecond)

	fileEntityMap, err := hdfsStruct.ListFiles()
	if nil != err {
		log.Println("HDFS watcher could not list files due to " + err.Error())
		hdfsStruct.FindWorkingNamenode()
		time.Sleep(time.Duration(hdfsStruct.GraceMilliSec) * time.Millisecond * 10)
		return nil, err
	}

	for prevKey, prevEnt := range prevFileEntityMap {
		if ent, ok := fileEntityMap[prevKey]; ok && prevEnt.Size == ent.Size {
			filteredFileEntityMap[prevKey] = ent
		}
	}

	return filteredFileEntityMap, nil
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
