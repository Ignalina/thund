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
	"fmt"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/ignalina/alloy/api"
	"github.com/ignalina/alloy/ffi/rust"
	"io"
)

type ArrayToRust struct {
}

func (atr *ArrayToRust) Process(reader io.Reader, customParams interface{}) bool {
	fmt.Println("dummy process")

	mem := memory.NewGoAllocator()

	bld0 := array.NewInt32Builder(mem)
	defer bld0.Release()
	bld0.AppendValues([]int32{122}, []bool{true})
	arr0 := bld0.NewInt32Array() // materialize the array
	defer arr0.Release()

	bld1 := array.NewInt64Builder(mem)
	defer bld1.Release()
	bld1.AppendValues([]int64{122}, []bool{true})
	arr1 := bld1.NewInt64Array() // materialize the array
	defer arr1.Release()

	listOfarrays := []arrow.Array{arr0, arr1}

	fmt.Printf("[Go]\tCalling the goBridge with:\n\tarr: %v\n", listOfarrays)

	//
	// !!! Envision the above arrays are read from a CSV , and we now call an rust.rs based step via alloy !!!
	//

	var b api.Bridge

	b = rust.Bridge{api.CommonParameter{mem}}

	i, err := b.FromChunks(listOfarrays)

	if nil != err {
		fmt.Println(err)
	} else {
		fmt.Printf("[Go]\tRust counted %v arrays sent through ffi\n", i)
	}

	return true
}

func (atr *ArrayToRust) Setup(customParams interface{}) bool {
	fmt.Println("dummy setup")
	return true
}
