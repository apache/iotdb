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
	"tsfile/common/constant"
)

type GorillaEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	//baseDecoder   Encoder
	flag               bool
	leadingZeroNum     int32
	tailingZeroNum     int32
	buffer             byte
	numberLeftInBuffer uint32
}

func (d *GorillaEncoder) writeBit(b bool, buffer *bytes.Buffer) {
	d.buffer <<= 1
	if b {
		d.buffer |= 1
	}
	d.numberLeftInBuffer++
	if d.numberLeftInBuffer == 8 {
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) writeIntBit(i int32, buffer *bytes.Buffer) {
	d.buffer <<= 1
	if i != 0 {
		d.buffer |= 1
		//d.writeBit(true, buffer)
	}
	d.numberLeftInBuffer++
	if d.numberLeftInBuffer == 8 {
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) writeLongBit(i int64, buffer *bytes.Buffer) {
	d.buffer <<= 1
	if i != 0 {
		d.buffer |= 1
		//d.writeBit(true, buffer)
	}
	d.numberLeftInBuffer++
	if d.numberLeftInBuffer == 8 {
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) Reset() {
	d.flag = false
	d.numberLeftInBuffer = 0
	d.buffer = 0
}

func (d *GorillaEncoder) CleanBuffer(buffer *bytes.Buffer) {
	if d.numberLeftInBuffer == 0 {
		return
	}
	if d.numberLeftInBuffer > 0 {
		d.buffer <<= (8 - d.numberLeftInBuffer)
	}
	buffer.WriteByte(d.buffer)
	d.numberLeftInBuffer = 0
	d.buffer = 0
}
