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
	"math"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type SinglePrecisionDecoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType
	reader     *utils.BytesReader
	flag       bool
	preValue   int32

	base GorillaDecoder
}

func (d *SinglePrecisionDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *SinglePrecisionDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *SinglePrecisionDecoder) Next() interface{} {
	if !d.flag {
		d.flag = true

		ch := d.reader.ReadSlice(4)
		d.preValue = int32(ch[0]) + int32(ch[1])<<8 + int32(ch[2])<<16 + int32(ch[3])<<24
		d.base.leadingZeroNum = utils.NumberOfLeadingZeros(d.preValue)
		d.base.tailingZeroNum = utils.NumberOfTrailingZeros(d.preValue)
		tmp := math.Float32frombits(uint32(d.preValue))
		d.base.fillBuffer(d.reader)
		d.getNextValue()

		return tmp
	} else {
		tmp := math.Float32frombits(uint32(d.preValue))
		d.getNextValue()

		return tmp
	}
}

func (d *SinglePrecisionDecoder) getNextValue() {
	// case: '0'
	if !d.base.readBit(d.reader) {
		return
	}
	if !d.base.readBit(d.reader) {
		// case: '10'
		var tmp int32 = 0
		for i := 0; i < conf.FLOAT_LENGTH-int(d.base.leadingZeroNum+d.base.tailingZeroNum); i++ {
			var bit int32
			if d.base.readBit(d.reader) {
				bit = 1
			} else {
				bit = 0
			}
			tmp |= bit << uint(conf.FLOAT_LENGTH-1-int(d.base.leadingZeroNum)-i)
		}
		tmp ^= d.preValue
		d.preValue = tmp
	} else {
		// case: '11'
		leadingZeroNumTmp := int(d.base.readIntFromStream(d.reader, conf.FLAOT_LEADING_ZERO_LENGTH))
		lenTmp := int(d.base.readIntFromStream(d.reader, conf.FLOAT_VALUE_LENGTH))
		var tmp int32 = d.base.readIntFromStream(d.reader, lenTmp)
		tmp <<= uint(conf.FLOAT_LENGTH - leadingZeroNumTmp - lenTmp)
		tmp ^= d.preValue
		d.preValue = tmp
	}
	d.base.leadingZeroNum = utils.NumberOfLeadingZeros(d.preValue)
	d.base.tailingZeroNum = utils.NumberOfTrailingZeros(d.preValue)
}

func NewSinglePrecisionDecoder(dataType constant.TSDataType) *SinglePrecisionDecoder {
	return &SinglePrecisionDecoder{dataType: dataType}
}
