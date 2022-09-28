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

package api

import "sort"

type FileEntity struct {
	Name      string
	UnixMilli int64
	Size      int64
	Lines     int64
	Sum       []byte
	Header    string
	Footer    string
	HashName  bool
	HasSize   bool
	HasLines  bool
	HasSum    bool
	HasHeader bool
	HasFooter bool
}

func SortOnAge(fileEntityMap map[string]FileEntity) []string {
	keys := make([]string, 0, len(fileEntityMap))
	for k := range fileEntityMap {
		keys = append(keys, k)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return fileEntityMap[keys[i]].UnixMilli < fileEntityMap[keys[j]].UnixMilli
	})
	return keys
}
