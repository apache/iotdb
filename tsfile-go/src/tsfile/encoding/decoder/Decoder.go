/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package decoder

import (
	_ "bytes"
	_ "os"
	"strconv"
	"tsfile/common/constant"
)

const (
	RLE        = 0
	BIT_PACKED = 1
)

type Decoder interface {
	Init(data []byte)
	HasNext() bool
	Next() interface{}
}

func CreateDecoder(encoding constant.TSEncoding, dataType constant.TSDataType) Decoder {
	// PLA and DFT encoding are not supported in current version
	var decoder Decoder

	switch {
	case encoding == constant.PLAIN:
		decoder = &PlainDecoder{dataType: dataType}
	case encoding == constant.RLE:
		if dataType == constant.BOOLEAN {
			decoder = NewIntRleDecoder(dataType)
		} else if dataType == constant.INT32 {
			decoder = NewIntRleDecoder(dataType)
		} else if dataType == constant.INT64 {
			decoder = NewLongRleDecoder(dataType)
		} else if dataType == constant.FLOAT {
			decoder = NewFloatDecoder(encoding, dataType)
		} else if dataType == constant.DOUBLE {
			decoder = NewDoubleDecoder(encoding, dataType)
		}
	case encoding == constant.TS_2DIFF:
		if dataType == constant.INT32 {
			decoder = NewIntDeltaDecoder(dataType)
		} else if dataType == constant.INT64 {
			decoder = NewLongDeltaDecoder(dataType)
		} else if dataType == constant.FLOAT {
			decoder = NewFloatDecoder(encoding, dataType)
		} else if dataType == constant.DOUBLE {
			decoder = NewDoubleDecoder(encoding, dataType)
		}
	case encoding == constant.GORILLA:
		if dataType == constant.FLOAT {
			decoder = NewSinglePrecisionDecoder(dataType)
		} else if dataType == constant.DOUBLE {
			decoder = NewDoublePrecisionDecoder(dataType)
		}
	default:
		panic("Decoder not found, encoding:" + strconv.Itoa(int(encoding)) + ", dataType:" + strconv.Itoa(int(dataType)))
	}

	return decoder
}
