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

type DoublePrecisionDecoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType
	reader     *utils.BytesReader
	flag       bool
	preValue   int64

	base GorillaDecoder
}

func (d *DoublePrecisionDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *DoublePrecisionDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *DoublePrecisionDecoder) Next() interface{} {
	if !d.flag {
		d.flag = true

		ch := d.reader.ReadSlice(8)
		var res int64 = 0
		for i := 0; i < 8; i++ {
			res += int64(ch[i]) << uint(i*8)
		}
		d.preValue = res

		d.base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
		d.base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
		tmp := math.Float64frombits(uint64(d.preValue))
		d.base.fillBuffer(d.reader)
		d.getNextValue()

		return tmp
	} else {
		tmp := math.Float64frombits(uint64(d.preValue))
		d.getNextValue()

		return tmp
	}
}

func (d *DoublePrecisionDecoder) getNextValue() {
	// case: '0'
	if !d.base.readBit(d.reader) {
		return
	}
	if !d.base.readBit(d.reader) {
		// case: '10'
		var tmp int64 = 0
		l := conf.DOUBLE_LENGTH - int(d.base.leadingZeroNum+d.base.tailingZeroNum)
		t := conf.DOUBLE_LENGTH - int(d.base.leadingZeroNum+1)
		for i := 0; i < l; i++ {
			var bit int
			if d.base.readBit(d.reader) {
				bit = 1
			} else {
				bit = 0
			}
			tmp |= int64(bit << uint(t-i))
		}
		tmp ^= d.preValue
		d.preValue = tmp
	} else {
		// case: '11'
		leadingZeroNumTmp := int(d.base.readIntFromStream(d.reader, conf.DOUBLE_LEADING_ZERO_LENGTH))
		lenTmp := int(d.base.readIntFromStream(d.reader, conf.DOUBLE_VALUE_LENGTH))
		var tmp int64 = d.base.readLongFromStream(d.reader, lenTmp)
		tmp <<= uint(conf.DOUBLE_LENGTH - leadingZeroNumTmp - lenTmp)
		tmp ^= d.preValue
		d.preValue = tmp
	}
	d.base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
	d.base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
}

func NewDoublePrecisionDecoder(dataType constant.TSDataType) *DoublePrecisionDecoder {
	return &DoublePrecisionDecoder{dataType: dataType}
}
