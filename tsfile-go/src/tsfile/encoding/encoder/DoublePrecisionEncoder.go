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
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type DoublePrecisionEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	base     *GorillaEncoder
	preValue int64
}

func (d *DoublePrecisionEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	base := d.base
	if !base.flag {
		// case: write first 8 byte value without any encoding
		base.flag = true
		d.preValue = int64(math.Float64bits(v.(float64)))
		base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
		base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
		binary.Write(buffer, binary.LittleEndian, d.preValue)
		//var bufferLittle []byte
		//bufferLittle = utils.Int64ToByte(d.preValue, 1)
		//buffer.Write(bufferLittle)
	} else {
		nextValue := int64(math.Float64bits(v.(float64)))
		tmp := nextValue ^ d.preValue
		if tmp == 0 {
			// case: write '0'
			//base.writeBit(false, buffer)
			base.buffer <<= 1
			base.numberLeftInBuffer++
			if base.numberLeftInBuffer == 8 {
				base.CleanBuffer(buffer)
			}
		} else {
			var bit int64 = 0
			var index int32 = 0
			var value int64
			leadingZeroNumTmp := utils.NumberOfLeadingZerosLong(tmp)
			tailingZeroNumTmp := utils.NumberOfTrailingZerosLong(tmp)
			if leadingZeroNumTmp >= base.leadingZeroNum && tailingZeroNumTmp >= base.tailingZeroNum {
				// case: write '10' and effective bits without first leadingZeroNum '0' and last tailingZeroNum '0'
				//base.writeBit(true, buffer)
				//base.writeBit(false, buffer)
				base.buffer <<= 1
				base.buffer |= 1
				base.numberLeftInBuffer++
				if base.numberLeftInBuffer == 8 {
					base.CleanBuffer(buffer)
				}
				base.buffer <<= 1
				base.numberLeftInBuffer++
				if base.numberLeftInBuffer == 8 {
					base.CleanBuffer(buffer)
				}

				//d.writeBits(tmp, buffer, int32(conf.DOUBLE_LENGTH)-1-base.leadingZeroNum, base.tailingZeroNum)
				for index = int32(conf.DOUBLE_LENGTH) - 1 - base.leadingZeroNum; index >= base.tailingZeroNum; index-- {
					bit = tmp & (1 << uint32(index))

					base.buffer <<= 1
					if bit != 0 {
						base.buffer |= 1
					}
					base.numberLeftInBuffer++
					if base.numberLeftInBuffer == 8 {
						base.CleanBuffer(buffer)
					}
				}
			} else {
				// case: write '11', leading zero num of value, effective bits len and effective bit value
				//d.base.writeBit(true, buffer)
				//d.base.writeBit(true, buffer)
				base.buffer <<= 1
				base.buffer |= 1
				base.numberLeftInBuffer++
				if base.numberLeftInBuffer == 8 {
					base.CleanBuffer(buffer)
				}
				base.buffer <<= 1
				base.buffer |= 1
				base.numberLeftInBuffer++
				if base.numberLeftInBuffer == 8 {
					base.CleanBuffer(buffer)
				}

				//d.writeBits(int64(leadingZeroNumTmp), buffer, int32(conf.DOUBLE_LEADING_ZERO_LENGTH)-1, 0)
				//d.writeBits(int64(int32(conf.DOUBLE_LENGTH)-leadingZeroNumTmp-tailingZeroNumTmp), buffer, int32(conf.DOUBLE_VALUE_LENGTH)-1, 0)
				//d.writeBits(tmp, buffer, int32(conf.DOUBLE_LENGTH)-1-leadingZeroNumTmp, tailingZeroNumTmp)
				value = int64(leadingZeroNumTmp)
				for index = int32(conf.DOUBLE_LEADING_ZERO_LENGTH) - 1; index >= 0; index-- {
					bit = value & (1 << uint32(index))

					base.buffer <<= 1
					if bit != 0 {
						base.buffer |= 1
					}
					base.numberLeftInBuffer++
					if base.numberLeftInBuffer == 8 {
						base.CleanBuffer(buffer)
					}
				}

				value = int64(int32(conf.DOUBLE_LENGTH) - leadingZeroNumTmp - tailingZeroNumTmp)
				for index = int32(conf.DOUBLE_VALUE_LENGTH) - 1; index >= 0; index-- {
					bit = value & (1 << uint32(index))

					base.buffer <<= 1
					if bit != 0 {
						base.buffer |= 1
					}
					base.numberLeftInBuffer++
					if base.numberLeftInBuffer == 8 {
						base.CleanBuffer(buffer)
					}
				}

				for index = int32(conf.DOUBLE_LENGTH) - 1 - leadingZeroNumTmp; index >= tailingZeroNumTmp; index-- {
					bit = tmp & (1 << uint32(index))

					base.buffer <<= 1
					if bit != 0 {
						base.buffer |= 1
					}
					base.numberLeftInBuffer++
					if base.numberLeftInBuffer == 8 {
						base.CleanBuffer(buffer)
					}
				}
			}
			d.preValue = nextValue
			base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
			base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
		}
	}
}

func (d *DoublePrecisionEncoder) Flush(buffer *bytes.Buffer) {
	d.Encode(math.Float64frombits(0x7ff8000000000000), buffer)
	d.base.CleanBuffer(buffer)
	d.base.Reset()
}

func (d *DoublePrecisionEncoder) GetMaxByteSize() int64 {
	// max(first 4 byte, case '11' bit + 5bit + 6bit + 32bit = 45bit) + NaN(case '11' bit + 5bit + 6bit + 32bit = 45bit) = 90bit
	return 20
}

func (d *DoublePrecisionEncoder) GetOneItemMaxSize() int {
	// case '11'
	// 2bit + 5bit + 6bit + 32bit = 45bit
	return 10
}

func (d *DoublePrecisionEncoder) writeBits(num int64, buffer *bytes.Buffer, start int32, end int32) {
	var bit int64 = 0
	var i int32 = 0
	base := d.base
	for i = start; i >= end; i-- {
		bit = num & (1 << uint32(i))

		base.buffer <<= 1
		if bit != 0 {
			base.buffer |= 1
		}
		base.numberLeftInBuffer++
		if base.numberLeftInBuffer == 8 {
			base.CleanBuffer(buffer)
		}
	}
}

func NewDoublePrecisionEncoder(dataType constant.TSDataType) *DoublePrecisionEncoder {
	//log.Info("double using Gorilla")
	d := &DoublePrecisionEncoder{dataType: dataType}
	d.base = &GorillaEncoder{}
	d.base.flag = false
	return d
}
