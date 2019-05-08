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
	_ "encoding/binary"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

// This package is a decoder for decoding the byte array that encoded by DeltaBinaryDecoder just supports integer and long values.
// 0-3 bits int32 存储数值个数
// 4-7 bits int32 存储单个数值宽度
// 8-15 bits int64 存储最小值，作为所有数值的基数
// 16-23 bits int64 存储第一个值
// 24 bit 之后存储数值
type LongDeltaDecoder struct {
	dataType constant.TSDataType
	reader   *utils.BytesReader

	count int
	width int
	index int

	baseValue     int64
	firstValue    int64
	decodedValues []int64
}

func (d *LongDeltaDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *LongDeltaDecoder) HasNext() bool {
	return (d.index < d.count) || (d.reader.Len() > 0)
}

func (d *LongDeltaDecoder) Next() interface{} {
	if d.index == d.count {
		return d.loadPack()
	} else {
		result := d.decodedValues[d.index]
		d.index++

		return result
	}
}

func (d *LongDeltaDecoder) loadPack() int64 {
	d.count = int(d.reader.ReadInt())
	d.width = int(d.reader.ReadInt())
	d.baseValue = d.reader.ReadLong()
	d.firstValue = d.reader.ReadLong()

	d.index = 0

	//how many bytes data takes after encoding
	encodingLength := int(math.Ceil(float64(d.count*d.width) / 8.0))
	valueBuffer := d.reader.ReadSlice(encodingLength)

	previousValue := d.firstValue
	d.decodedValues = make([]int64, d.count)
	for i := 0; i < d.count; i++ {
		p := d.width * i
		v := utils.BytesToLong(valueBuffer, p, d.width)
		d.decodedValues[i] = previousValue + d.baseValue + v

		previousValue = d.decodedValues[i]
	}

	return d.firstValue
}

func NewLongDeltaDecoder(dataType constant.TSDataType) *LongDeltaDecoder {
	return &LongDeltaDecoder{dataType: dataType}
}
