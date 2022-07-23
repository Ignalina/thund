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
	"fmt"
	"github.com/ignalina/thund/api"
	"io"
	"log"
	"os"
)

type FetchAndExecEvent struct {
	destinationFolder string
}

func (fae FetchAndExecEvent) Process(reader io.Reader, customParams interface{}) bool {
	fmt.Println("Fetch And Exec Process")
	// download whole file , do checksum , cal lines ,extract header/footer
	fe :=customParams.( api.FileEntity)

	file, err := os.Open(fae.destinationFolder+fe.Name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if !fe.HasLines {
		io.Copy(file,reader)
	} else {
		var lineCnt int64
		scanner := bufio.NewScanner(reader)
		// optionally, resize scanner's capacity for lines over 64K, see next example
		for scanner.Scan() {
			file.Write([]byte(scanner.Text()))
			lineCnt+=1
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		fe.Lines=lineCnt
	}
	return true
}
func (fae FetchAndExecEvent) Setup(customParams interface{}) bool {
	fmt.Println("Fetch And Exec setup")
	return true
}
