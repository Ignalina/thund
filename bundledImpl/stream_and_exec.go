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
	"bufio"
	"bytes"
	"fmt"
	"github.com/ignalina/thund/api"
	"io"
	"log"
	"os/exec"
	"text/template"
)

type StreamAndExecEvent struct {
	CmdTemplate string
	Data        map[string]interface{}
	Tmpl        *template.Template
}

func (sae *StreamAndExecEvent) Process(reader io.Reader, customParams interface{}) bool {
	fmt.Println("Stream And Exec Process")
	// while streaming if configured do checksum , cal lines ,extract header/footer
	fe := customParams.(api.FileEntity)
	pipeReader, pipeWriter := io.Pipe()

	defer pipeWriter.Close()

	var err error
	sae.Data["path"] = fe.Name
	buf := &bytes.Buffer{}
	err = sae.Tmpl.Execute(buf, sae.Data)
	if err != nil {
		panic(err)
	}

	cmd := exec.Command(buf.String())
	cmdInPipe, err := cmd.StdinPipe()
	var bytesBuffer bytes.Buffer
	goon := true

	// Pipe in data in CMD until we streamed all data given to use
	go func() {
		for goon {
			_, eof := pipeReader.Read(bytesBuffer.Bytes())
			goon = (eof == io.EOF)
			cmdInPipe.Write(bytesBuffer.Bytes())
		}
	}()

	if !fe.HasLines {

		fe.Size, err = io.Copy(pipeWriter, reader)

		if err != nil {
			return false
		}

		pipeWriter.Close()
	} else {
		var lineCnt int64
		scanner := bufio.NewScanner(reader)

		for scanner.Scan() {
			var n int
			n, err = pipeWriter.Write([]byte(scanner.Text()))
			fe.Size += int64(n)
			lineCnt += 1
		}

		if err := scanner.Err(); err != nil {
			log.Println(err)
			return false
		}
		fe.Lines = lineCnt
	}

	// Streaming is done , collect return values from process

	data, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	for k, v := range data {
		fmt.Printf("key :  %v, value :  %v \n", k, string(v))
	}

	return true
}

func (sae *StreamAndExecEvent) Setup(customParams interface{}) bool {
	fmt.Println("Fetch And Exec setup")

	jsonTemplate := sae.CmdTemplate

	sae.Data = make(map[string]interface{}, 8)
	sae.Tmpl = template.Must(template.New("command.tmpl").Parse(jsonTemplate))

	sae.Data["traceid"] = "123"
	sae.Data["linecount"] = "FilMeIn"
	sae.Data["path"] = "FillMeIn"
	sae.Data["sum"] = "FillMeIn"

	// TODO Makesure destination directory exists or create it.
	return true
}
