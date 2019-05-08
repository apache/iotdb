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
	"tsfile/common/utils"
)

type BitmapEncoder struct {
	tsDataType   constant.TSDataType
	endianType   int8
	encodeEndian int8
	values       []int32
}

func (this *BitmapEncoder) Encode(value interface{}, buffer *bytes.Buffer) {
	switch this.tsDataType {
	case (constant.INT32):
		if data, ok := value.(int32); ok {
			this.values = append(this.values, data)
		}
		break
	default:
		break
	}

}

func (this *BitmapEncoder) Flush(buffer *bytes.Buffer) {
	var byteCache *bytes.Buffer
	valueType := this.values //make([]int32,this.values...)//new HashSet<Integer>(values)
	len := len(this.values)
	byteNum := (len + 7) / 8
	if byteNum == 0 {
		this.reset()
		return
	}
	for _, value := range valueType {
		buffer := make([]byte, byteNum)
		for i := 0; i < len; i++ {
			if this.values[i] == value {
				index := i / 8
				offset := 7 - (i % 8)
				buffer[index] = (buffer[index] | (byte(1) << uint(offset)))
			}
		}
		utils.WriteUnsignedVarInt(value, byteCache)
		byteCache.Write(buffer)
	}
	utils.WriteUnsignedVarInt(int32(byteCache.Len()), buffer)
	utils.WriteUnsignedVarInt(int32(len), buffer)
	buffer.Write(byteCache.Bytes())
	this.reset()

}

func (this *BitmapEncoder) GetMaxByteSize() int64 {
	return int64((4 + 4 + (len(this.values)+7)/8 + 4) * len(this.values))
}

func (this *BitmapEncoder) GetOneItemMaxSize() int {
	return 1
}

func (this *BitmapEncoder) reset() {
	this.values = this.values[0:0]
}

func NewBitmapEncoder(tdt constant.TSDataType, endianType int8) (*BitmapEncoder, error) {
	return &BitmapEncoder{
		tsDataType:   tdt,
		endianType:   endianType,
		encodeEndian: 1,
	}, nil
}
