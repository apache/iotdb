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
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/common/utils"
)

type SinglePrecisionEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	base     *GorillaEncoder
	preValue int32
}

func (d *SinglePrecisionEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	base := d.base
	if !base.flag {
		base.flag = true
		d.preValue = int32(math.Float32bits(v.(float32)))
		base.leadingZeroNum = utils.NumberOfLeadingZeros(d.preValue)
		base.tailingZeroNum = utils.NumberOfTrailingZeros(d.preValue)
		buffer.Write(utils.Int32ToByte(d.preValue, 1))

		//buffer.WriteByte(byte((d.preValue >> 0) & 0xFF))
		//buffer.WriteByte(byte((d.preValue >> 8) & 0xFF))
		//buffer.WriteByte(byte((d.preValue >> 16) & 0xFF))
		//buffer.WriteByte(byte((d.preValue >> 24) & 0xFF))
	} else {
		var nextValue int32
		var tmp int32
		var bit int32 = 0
		var index int32 = 0
		var value int32
		nextValue = int32(math.Float32bits(v.(float32)))
		tmp = nextValue ^ d.preValue
		if tmp == 0 {
			//base.writeBit(false, buffer)
			base.buffer <<= 1
			base.numberLeftInBuffer++
			if base.numberLeftInBuffer == 8 {
				base.CleanBuffer(buffer)
			}
		} else {
			var leadingZeroNumTmp int32
			var tailingZeroNumTmp int32
			leadingZeroNumTmp = utils.NumberOfLeadingZeros(tmp)
			tailingZeroNumTmp = utils.NumberOfTrailingZeros(tmp)
			if leadingZeroNumTmp >= base.leadingZeroNum && tailingZeroNumTmp >= d.base.tailingZeroNum {
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

				//d.writeBits(tmp, buffer, int32(conf.FLOAT_LENGTH)-1-base.leadingZeroNum, base.tailingZeroNum)
				for index = int32(conf.FLOAT_LENGTH) - 1 - base.leadingZeroNum; index >= base.tailingZeroNum; index-- {
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
				//base.writeBit(true, buffer)
				//base.writeBit(true, buffer)
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

				//d.writeBits(leadingZeroNumTmp, buffer, int32(conf.FLAOT_LEADING_ZERO_LENGTH)-1, 0)
				//d.writeBits(int32(conf.FLOAT_LENGTH)-leadingZeroNumTmp-tailingZeroNumTmp, buffer, int32(conf.FLOAT_VALUE_LENGTH)-1, 0)
				//d.writeBits(tmp, buffer, int32(conf.FLOAT_LENGTH)-1-leadingZeroNumTmp, tailingZeroNumTmp)
				for index = int32(conf.FLAOT_LEADING_ZERO_LENGTH) - 1; index >= 0; index-- {
					bit = leadingZeroNumTmp & (1 << uint32(index))
					base.buffer <<= 1
					if bit != 0 {
						base.buffer |= 1
					}
					base.numberLeftInBuffer++
					if base.numberLeftInBuffer == 8 {
						base.CleanBuffer(buffer)
					}
				}
				value = int32(conf.FLOAT_LENGTH) - leadingZeroNumTmp - tailingZeroNumTmp
				for index = int32(conf.FLOAT_VALUE_LENGTH) - 1; index >= 0; index-- {
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
				for index = int32(conf.FLOAT_LENGTH) - 1 - leadingZeroNumTmp; index >= tailingZeroNumTmp; index-- {
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
			base.leadingZeroNum = utils.NumberOfLeadingZeros(d.preValue)
			base.tailingZeroNum = utils.NumberOfTrailingZeros(d.preValue)
		}
	}
}

func (d *SinglePrecisionEncoder) Flush(buffer *bytes.Buffer) {
	d.Encode(math.Float32frombits(0x7fc00000), buffer)
	d.base.CleanBuffer(buffer)
	d.base.Reset()
}

func (d *SinglePrecisionEncoder) GetMaxByteSize() int64 {
	// max(first 4 byte, case '11' bit + 5bit + 6bit + 32bit = 45bit) + NaN(case '11' bit + 5bit + 6bit + 32bit = 45bit) = 90bit
	return 12
}

func (d *SinglePrecisionEncoder) GetOneItemMaxSize() int {
	// case '11'
	// 2bit + 5bit + 6bit + 32bit = 45bit
	return 6
}

func (d *SinglePrecisionEncoder) writeBits(num int32, buffer *bytes.Buffer, start int32, end int32) {
	var bit int32 = 0
	var index int32 = 0
	base := d.base
	for index = start; index >= end; index-- {
		bit = num & (1 << uint32(index))
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

func NewSinglePrecisionEncoder(dataType constant.TSDataType) *SinglePrecisionEncoder {
	log.Info("float using Gorilla")
	d := &SinglePrecisionEncoder{dataType: dataType}
	d.base = &GorillaEncoder{}
	d.base.flag = false
	return d
}
