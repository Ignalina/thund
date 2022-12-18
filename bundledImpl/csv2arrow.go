package bundledImpl

import (
	"fmt"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/csv"
	"github.com/ignalina/thund/api"
	"io"
)

type CsvEvent struct {
	Schema      *arrow.Schema
	withComment byte
	WithComma   byte
}

func (ce *CsvEvent) Process(reader io.Reader, customParams interface{}) bool {

	cp := customParams
	var fe api.FileEntity
	fe = cp.(api.FileEntity)
	fmt.Println(fe)

	r := csv.NewReader(reader, ce.Schema, csv.WithComment('#'), csv.WithComma(';'))
	defer r.Release()

	//	ii := de.FST.CustomParams

	//	var h ShbGlue.Hbomparams
	//	h = ii.(ShbGlue.Hbomparams)
	return true
}

func (ce *CsvEvent) Setup(customParams interface{}) bool {
	fmt.Println("csv2arrow setup")

	// TODO generate schema from json schema
	ce.Schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	return true
}
