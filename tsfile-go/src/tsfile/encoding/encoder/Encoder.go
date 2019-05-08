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

package encoder

import (
	"bytes"
	"strconv"
	"tsfile/common/conf"
	"tsfile/common/constant"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-9-28 下午5:55
 * @Description:
 */

type Encoder interface {
	Encode(value interface{}, buffer *bytes.Buffer)
	Flush(buffer *bytes.Buffer)
	GetOneItemMaxSize() int
	GetMaxByteSize() int64
}

func GetEncoder(et int16, tdt int16) Encoder {
	encoding := constant.TSEncoding(et)
	dataType := constant.TSDataType(tdt)

	var encoder Encoder
	switch {
	case encoding == constant.PLAIN:
		encoder, _ = NewPlainEncoder(dataType)
	case encoding == constant.RLE:
		if dataType == constant.INT32 {
			encoder = NewRleEncoder(constant.INT32)
		} else if dataType == constant.INT64 {
			encoder = NewRleEncoder(constant.INT64)
		} else if dataType == constant.FLOAT || dataType == constant.DOUBLE {
			encoder = NewFloatEncoder(encoding, int32(conf.FloatPrecision), dataType)
		}
	case encoding == constant.TS_2DIFF:
		if dataType == constant.INT32 {
			encoder = NewIntDeltaEncoder(constant.INT32)
		} else if dataType == constant.INT64 {
			encoder = NewLongDeltaEncoder(constant.INT32)
		} else if dataType == constant.DOUBLE {
			encoder = NewDoubleDeltaEncoder(encoding, conf.FloatPrecision, dataType)
			//encoder = NewFloatEncoder(encoding, conf.FloatPrecision, dataType)
		} else if dataType == constant.FLOAT {
			encoder = NewFloatDeltaEncoder(encoding, conf.FloatPrecision, dataType)
			//encoder = NewFloatEncoder(encoding, conf.FloatPrecision, dataType)
		}
	case encoding == constant.GORILLA:
		if dataType == constant.FLOAT {
			encoder = NewSinglePrecisionEncoder(dataType)
		} else if dataType == constant.DOUBLE {
			encoder = NewDoublePrecisionEncoder(dataType)
		}

	default:
		panic("Encoder not found, encoding:" + strconv.Itoa(int(encoding)) + ", dataType:" + strconv.Itoa(int(dataType)))
	}

	return encoder
}
