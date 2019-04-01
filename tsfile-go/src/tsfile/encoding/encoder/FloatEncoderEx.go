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
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	maxPointNumber          int32
	maxPointNumberSavedFlag bool
	maxPointValue           float64

	baseEncoder *IntDeltaEncoder
}

func NewFloatDeltaEncoder(encoding constant.TSEncoding, maxPointNumber int, dataType constant.TSDataType) *FloatDeltaEncoder {
	d := &FloatDeltaEncoder{
		dataType:                dataType,
		maxPointNumber:          int32(maxPointNumber),
		maxPointNumberSavedFlag: false,
		maxPointValue:           0,
	}
	d.baseEncoder = NewIntDeltaEncoder(dataType)

	if d.maxPointNumber <= 0 {
		d.maxPointNumber = 0
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow10(maxPointNumber)
	}
	return d
}

func (d *FloatDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(d.maxPointNumber, buffer)
		d.maxPointNumberSavedFlag = true
	}
	value := (int32)(math.Round(float64(v.(float32)) * d.maxPointValue))
	d.baseEncoder.Encode(value, buffer)
}

func (d *FloatDeltaEncoder) Flush(buffer *bytes.Buffer) {
	d.baseEncoder.Flush(buffer)
}

func (d *FloatDeltaEncoder) GetMaxByteSize() int64 {
	return d.baseEncoder.GetMaxByteSize()
}

func (d *FloatDeltaEncoder) GetOneItemMaxSize() int {
	return d.baseEncoder.GetOneItemMaxSize()
}

type DoubleDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	maxPointNumber          int32
	maxPointNumberSavedFlag bool
	maxPointValue           float64

	baseEncoder *LongDeltaEncoder
}

func NewDoubleDeltaEncoder(encoding constant.TSEncoding, maxPointNumber int, dataType constant.TSDataType) *DoubleDeltaEncoder {
	d := &DoubleDeltaEncoder{
		dataType:                dataType,
		maxPointNumber:          int32(maxPointNumber),
		maxPointNumberSavedFlag: false,
		maxPointValue:           0,
	}
	d.baseEncoder = NewLongDeltaEncoder(dataType)

	if d.maxPointNumber <= 0 {
		d.maxPointNumber = 0
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow10(maxPointNumber)
	}
	return d
}

func (d *DoubleDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(d.maxPointNumber, buffer)
		d.maxPointNumberSavedFlag = true
	}
	//value := (int64)(math.Round(v.(float64) * d.maxPointValue))
	d.baseEncoder.Encode((int64)(math.Round(v.(float64)*d.maxPointValue)), buffer)
}

func (d *DoubleDeltaEncoder) Flush(buffer *bytes.Buffer) {
	d.baseEncoder.Flush(buffer)
}

func (d *DoubleDeltaEncoder) GetMaxByteSize() int64 {
	return d.baseEncoder.GetMaxByteSize()
}

func (d *DoubleDeltaEncoder) GetOneItemMaxSize() int {
	return d.baseEncoder.GetOneItemMaxSize()
}
