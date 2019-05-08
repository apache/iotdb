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
	"encoding/binary"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type LongDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	blockSize int32
	width     int32
	index     int32

	baseValue     int64
	firstValue    int64
	previousValue int64
	encodedValues []int64
}

func (d *LongDeltaEncoder) Init() {
	d.blockSize = BLOCK_DEFAULT_SIZE
	d.index = -1
	d.firstValue = 0
	d.previousValue = 0
	d.baseValue = math.MaxInt64
	d.encodedValues = make([]int64, d.blockSize)
}

func (d *LongDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	value := v.(int64)

	if d.index == -1 {
		d.index++
		d.firstValue = value
		d.previousValue = d.firstValue
		return
	}

	// calculate delta
	delta := value - d.previousValue
	if delta < d.baseValue {
		d.baseValue = delta
	}
	d.encodedValues[d.index] = delta
	d.index++

	d.previousValue = value
	if d.index == d.blockSize {
		d.Flush(buffer)
	}
}

func (d *LongDeltaEncoder) Flush(buffer *bytes.Buffer) {
	if d.index != -1 {
		index := d.index
		// since we store the min delta, the deltas will be converted to be the difference to min delta and all positive
		for i := int32(0); i < index; i++ {
			d.encodedValues[i] = d.encodedValues[i] - d.baseValue
		}

		width := int32(0)
		for i := int32(0); i < index; i++ {
			valueWidth := int32(64) - utils.NumberOfLeadingZerosLong(d.encodedValues[i])
			if valueWidth > width {
				width = valueWidth
			}
		}

		d.width = width

		//write header
		binary.Write(buffer, binary.BigEndian, index)
		binary.Write(buffer, binary.BigEndian, width)
		binary.Write(buffer, binary.BigEndian, d.baseValue)
		binary.Write(buffer, binary.BigEndian, d.firstValue)

		//write data with min width
		if encodingLength := int(math.Ceil(float64(index*d.width) / 8.0)); encodingLength > 0 {
			encodingBlockBuffer := make([]byte, encodingLength)
			var temp1 int32 = width - 1
			var temp2 int32
			var temp3 int32
			for i := int32(0); i < index; i++ {
				//LongToBytes(d.encodedValues[i], encodingBlockBuffer, width*i, width)
				temp2 = temp1
				for j := int32(0); j < width; j++ {
					temp3 = temp2 / 8
					if (d.encodedValues[i] & (int64(1) << uint32(j%64))) != 0 {
						encodingBlockBuffer[temp3] = (byte)(encodingBlockBuffer[temp3] | (1 << uint32(7-temp2%8)))
					} else {
						encodingBlockBuffer[temp3] = (byte)(encodingBlockBuffer[temp3] & ^(1 << uint32(7-temp2%8)))
					}
					temp2--
				}
				temp1 += width
				//utils.LongToBytes(d.encodedValues[i], encodingBlockBuffer, int(d.width*i), int(d.width))
				//binary.Write(buffer, binary.BigEndian, d.encodedValues[i])
			}
			buffer.Write(encodingBlockBuffer)
		}

		d.reset()
	}
}

func (d *LongDeltaEncoder) GetMaxByteSize() int64 {
	return int64(24 + d.index*8)
}

func (d *LongDeltaEncoder) GetOneItemMaxSize() int {
	return 8
}

func (d *LongDeltaEncoder) reset() {
	d.blockSize = BLOCK_DEFAULT_SIZE
	d.index = -1
	d.firstValue = 0
	d.previousValue = 0
	d.baseValue = math.MaxInt64
	d.encodedValues = make([]int64, d.blockSize)
}

func NewLongDeltaEncoder(dataType constant.TSDataType) *LongDeltaEncoder {
	d := &LongDeltaEncoder{dataType: dataType}
	d.reset()

	return d
}
