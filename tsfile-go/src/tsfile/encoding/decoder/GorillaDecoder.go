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
	_ "tsfile/common/constant"
	"tsfile/common/utils"
)

type GorillaDecoder struct {
	leadingZeroNum, tailingZeroNum int32
	buffer                         int32
	numberLeftInBuffer             int32
}

func (g *GorillaDecoder) readBit(reader *utils.BytesReader) bool {
	if g.numberLeftInBuffer == 0 {
		g.fillBuffer(reader)
	}

	g.numberLeftInBuffer--
	return ((g.buffer >> uint32(g.numberLeftInBuffer)) & 1) == 1
}

func (g *GorillaDecoder) fillBuffer(reader *utils.BytesReader) {
	g.buffer = reader.Read()
	g.numberLeftInBuffer = 8
}

func (g *GorillaDecoder) readIntFromStream(reader *utils.BytesReader, len int) int32 {
	var num int32 = 0
	for i := 0; i < len; i++ {
		var bit int32
		if g.readBit(reader) {
			bit = 1
		} else {
			bit = 0
		}
		num |= bit << uint(len-1-i)
	}
	return num
}

func (g *GorillaDecoder) readLongFromStream(reader *utils.BytesReader, len int) int64 {
	var num int64 = 0
	for i := 0; i < len; i++ {
		var bit int64
		if g.readBit(reader) {
			bit = 1
		} else {
			bit = 0
		}
		num |= bit << uint(len-1-i)
	}
	return num
}
