package utils

import "fmt"

type tSDataType struct {
	BOOLEAN int32
	INT32   int32
	INT64   int32
	FLOAT   int32
	DOUBLE  int32
	TEXT    int32
}

type tSEncoding struct {
	PLAIN            int32
	PLAIN_DICTIONARY int32
	RLE              int32
	DIFF             int32
	TS_2DIFF         int32
	BITMAP           int32
	GORILLA_V1       int32
	REGULAR          int32
	GORILLA          int32
}

type compressor struct {
	UNCOMPRESSED int32
	SNAPPY       int32
	GZIP         int32
	LZO          int32
	SDT          int32
	PAA          int32
	PLA          int32
	LZ4          int32
}

var TSDataType tSDataType
var TSEncoding tSEncoding
var Compressor compressor

func GetTSDataTypeFromStringList(string_List []string) *[]int32 {
	r := make([]int32, 0)
	for _, v := range string_List {
		switch v {
		case "BOOLEAN":
			r = append(r, TSDataType.BOOLEAN)
		case "INT32":
			r = append(r, TSDataType.INT32)
		case "INT64":
			r = append(r, TSDataType.INT64)
		case "FLOAT":
			r = append(r, TSDataType.FLOAT)
		case "DOUBLE":
			r = append(r, TSDataType.DOUBLE)
		case "TEXT":
			r = append(r, TSDataType.TEXT)
		default:
			fmt.Println("Unsupported dataType {%v}\n", v)
			return nil
		}
	}
	return &r
}

func init() {
	// constants for TSDataType
	TSDataType.BOOLEAN = 0
	TSDataType.INT32 = 1
	TSDataType.INT64 = 2
	TSDataType.FLOAT = 3
	TSDataType.DOUBLE = 4
	TSDataType.TEXT = 5

	// constants for TSEncoding
	TSEncoding.PLAIN = 0
	TSEncoding.PLAIN_DICTIONARY = 1
	TSEncoding.RLE = 2
	TSEncoding.DIFF = 3
	TSEncoding.TS_2DIFF = 4
	TSEncoding.BITMAP = 5
	TSEncoding.GORILLA_V1 = 6
	TSEncoding.REGULAR = 7
	TSEncoding.GORILLA = 8

	// constants for Compressor
	Compressor.UNCOMPRESSED = 0
	Compressor.SNAPPY = 1
	Compressor.GZIP = 2
	Compressor.LZO = 3
	Compressor.SDT = 4
	Compressor.PAA = 5
	Compressor.PLA = 6
	Compressor.LZ4 = 7

}
