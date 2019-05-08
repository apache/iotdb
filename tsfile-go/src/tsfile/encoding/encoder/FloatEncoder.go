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
	"math"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	baseEncoder             Encoder
	maxPointNumber          int32
	maxPointNumberSavedFlag bool
	maxPointValue           float64
}

func (d *FloatEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(int32(d.maxPointNumber), buffer)
		d.maxPointNumberSavedFlag = true
	}

	if d.dataType == constant.FLOAT {
		value := v.(float32)
		valueInt := int32(utils.Round(float64(value)*d.maxPointValue, 0))
		d.baseEncoder.Encode(valueInt, buffer)
	} else if d.dataType == constant.DOUBLE {
		value := v.(float64)
		valueLong := int64(utils.Round(float64(value)*d.maxPointValue, 0))
		d.baseEncoder.Encode(valueLong, buffer)
	} else {
		panic("invalid data type in FloatEncoder")
	}
}

func (d *FloatEncoder) Flush(buffer *bytes.Buffer) {
	d.baseEncoder.Flush(buffer)
}

func (d *FloatEncoder) GetMaxByteSize() int64 {
	return d.baseEncoder.GetMaxByteSize()
}

func (d *FloatEncoder) GetOneItemMaxSize() int {
	return d.baseEncoder.GetOneItemMaxSize()
}

func NewFloatEncoder(encoding constant.TSEncoding, maxPointNumber int32, dataType constant.TSDataType) *FloatEncoder {
	d := &FloatEncoder{dataType: dataType}

	if encoding == constant.RLE {
		if dataType == constant.FLOAT {
			d.baseEncoder = NewRleEncoder(constant.INT32)
		} else if dataType == constant.DOUBLE {
			d.baseEncoder = NewRleEncoder(constant.INT64)
		} else {
			panic("data type is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else if encoding == constant.TS_2DIFF {
		if dataType == constant.FLOAT {
			d.baseEncoder = NewIntDeltaEncoder(dataType)
		} else if dataType == constant.DOUBLE {
			d.baseEncoder = NewLongDeltaEncoder(dataType)
		} else {
			panic("data type is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else {
		panic("encoding is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
	}

	d.maxPointNumber = maxPointNumber
	if d.maxPointNumber <= 0 {
		d.maxPointNumber = 0
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow10(int(maxPointNumber))
	}

	d.maxPointNumberSavedFlag = false

	return d
}
