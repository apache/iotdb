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
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/common/utils"
	"tsfile/encoding/bitpacking"
)

type RleEncoder struct {
	tsDataType          constant.TSDataType
	endianType          int16
	encodeEndian        int16
	values_32           []int32
	values_64           []int64
	bitWidth            int
	repeatCount         int
	bitPackedGroupCount int
	bytesBuffer         []byte
	isBitPackRun        bool
	preValue_32         int32
	preValue_64         int64
	isBitWidthSaved     bool
	byteCache           *bytes.Buffer
	packer_32           *bitpacking.IntPacker
	packer_64           *bitpacking.LongPacker
	numBufferedValues   int
	bufferedValues_32   []int32
	bufferedValues_64   []int64
}

func (this *RleEncoder) Encode(value interface{}, buffer *bytes.Buffer) {
	//log.Info("enter RleEncoder!!")
	switch {
	case this.tsDataType == (constant.BOOLEAN):
		if data, ok := value.(bool); ok {
			this.EncBool(data, buffer)
		}
		return
	case this.tsDataType == (constant.INT32):
		if data, ok := value.(int32); ok {
			this.EncInt32(data, buffer)
		}
		return
	case this.tsDataType == (constant.INT64):
		if data, ok := value.(int64); ok {
			this.EncInt64(data, buffer)
		}
		return
	default:
		log.Error("invalid input encode type: %d", this.tsDataType)
	}
	return
}

func (this *RleEncoder) EncBool(value bool, buffer *bytes.Buffer) {
	if value {
		this.EncInt32(1, buffer)
	} else {
		this.EncInt32(0, buffer)
	}
}

func (this *RleEncoder) EncInt32(value int32, buffer *bytes.Buffer) {
	this.values_32 = append(this.values_32, value)
}

func (this *RleEncoder) EncInt64(value int64, buffer *bytes.Buffer) {
	this.values_64 = append(this.values_64, value)
}

func getIntMaxBitWidth(list []int32) int {
	max := int32(1)
	for _, num := range list {
		bitWidth := 32 - utils.NumberOfLeadingZeros(num)
		if bitWidth > max {
			max = bitWidth
		}
	}
	return int(max)
}

func getLongMaxBitWidth(list []int64) int {
	max := int32(1)
	for _, num := range list {
		bitWidth := 64 - utils.NumberOfLeadingZerosLong(num)
		if bitWidth > max {
			max = bitWidth
		}
	}
	return int(max)
}

func (this *RleEncoder) endPreviousBitPackedRun(lastBitPackedNum int32) {
	if !this.isBitPackRun {
		return
	}
	bitPackHeader := (byte)((this.bitPackedGroupCount << 1) | 1)
	this.byteCache.Write([]byte{bitPackHeader})
	utils.WriteUnsignedVarInt(lastBitPackedNum, this.byteCache)
	for _, bytes := range this.bytesBuffer {
		this.byteCache.Write([]byte{bytes})
	}
	this.bytesBuffer = this.bytesBuffer[0:0]
	this.isBitPackRun = false
	this.bitPackedGroupCount = 0
}

func (this *RleEncoder) writeRleRun() {
	this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM))
	utils.WriteUnsignedVarInt(int32(this.repeatCount<<1), this.byteCache)
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		utils.WriteIntLittleEndianPaddedOnBitWidth((this.preValue_32), this.byteCache, this.bitWidth)
		break
	case (constant.INT64):
		utils.WriteLongLittleEndianPaddedOnBitWidth(this.preValue_64, this.byteCache, this.bitWidth)
		break
	default:
		break
	}
	this.repeatCount = 0
	this.numBufferedValues = 0
}

func (this *RleEncoder) convertBuffer() {
	bytes := make([]byte, this.bitWidth)
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		tmpBuffer := make([]int32, conf.RLE_MIN_REPEATED_NUM)
		for i := 0; i < conf.RLE_MIN_REPEATED_NUM; i++ {
			if i < len(this.bufferedValues_32) {
				tmpBuffer[i] = (this.bufferedValues_32[i])
			}
		}
		this.packer_32.Pack8Values(tmpBuffer, 0, bytes)
		this.bytesBuffer = append(this.bytesBuffer, bytes...)
		break
	case (constant.INT64):
		tmpBuffer := make([]int64, 0) //conf.RLE_MIN_REPEATED_NUM)
		for i := 0; i < conf.RLE_MIN_REPEATED_NUM; i++ {
			if i < len(this.bufferedValues_64) {
				tmpBuffer = append(tmpBuffer, this.bufferedValues_64[i])
			} else {
				tmpBuffer = append(tmpBuffer, 0)
			}
		}
		this.packer_64.Pack8Values(tmpBuffer, 0, bytes)
		this.bytesBuffer = append(this.bytesBuffer, bytes...)
		break
	default:
		return
	}
}

func (this *RleEncoder) writeOrAppendBitPackedRun() {
	if this.bitPackedGroupCount >= conf.RLE_MAX_BIT_PACKED_NUM {
		this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM))
	}
	if !this.isBitPackRun {
		this.isBitPackRun = true
	}
	this.convertBuffer()
	this.numBufferedValues = 0
	this.repeatCount = 0
	this.bitPackedGroupCount = this.bitPackedGroupCount + 1
}

func (this *RleEncoder) encodeValue(v int64) {
	if !this.isBitWidthSaved {
		utils.WriteUnsignedVarInt(int32(this.bitWidth), this.byteCache)
		this.isBitWidthSaved = true
	}
	b := false
	is32 := false
	if this.tsDataType == constant.INT32 || this.tsDataType == constant.BOOLEAN {
		is32 = true
	} else {
		is32 = false
	}
	if is32 && int32(v) == (this.preValue_32) {
		b = true
	}
	if !is32 && int64(v) == (this.preValue_64) {
		b = true
	}
	if b {
		this.repeatCount++
		if this.repeatCount >= conf.RLE_MIN_REPEATED_NUM && this.repeatCount <= conf.RLE_MAX_REPEATED_NUM {
			return
		} else if this.repeatCount == conf.RLE_MAX_REPEATED_NUM+1 {
			this.repeatCount = conf.RLE_MAX_REPEATED_NUM
			this.writeRleRun()
			this.repeatCount = 1
			if is32 {
				this.preValue_32 = int32(v)
			} else {
				this.preValue_64 = v
			}
		}
	} else {
		if this.repeatCount >= conf.RLE_MIN_REPEATED_NUM {
			this.writeRleRun()
		}
		this.repeatCount = 1
		if is32 {
			this.preValue_32 = int32(v)
		} else {
			this.preValue_64 = v
		}
	}
	if is32 {
		this.bufferedValues_32 = append(this.bufferedValues_32, int32(v))
	} else {
		this.bufferedValues_64 = append(this.bufferedValues_64, v)
	}
	this.numBufferedValues++
	if this.numBufferedValues == conf.RLE_MIN_REPEATED_NUM {
		this.writeOrAppendBitPackedRun()
	}
}

func (this *RleEncoder) clearBuffer() {
	for i := this.numBufferedValues; i < conf.RLE_MIN_REPEATED_NUM; i++ {
		switch this.tsDataType {
		case (constant.BOOLEAN):
		case (constant.INT32):
			if i < len(this.bufferedValues_32) {
				this.bufferedValues_32 = append(this.bufferedValues_32, 0)
			}
			break
		case (constant.INT64):
			if i < len(this.bufferedValues_64) {
				this.bufferedValues_64 = append(this.bufferedValues_64, 0)
			}
			break
		default:
			break
		}
	}
}

func (this *RleEncoder) reset() {
	this.numBufferedValues = 0
	this.repeatCount = 0
	this.bitPackedGroupCount = 0
	this.bytesBuffer = this.bytesBuffer[0:0]
	this.isBitPackRun = false
	this.isBitWidthSaved = false
	this.byteCache.Reset() // = this.byteCache[0:0]
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		this.values_32 = this.values_32[0:0]
		this.preValue_32 = 0 //this.preValue_32[0:0]
		break
	case (constant.INT64):
		this.values_64 = this.values_64[0:0]
		this.preValue_64 = int64(0) //this.preValue_64[0:0]
		break
	default:
		break
	}
}

func (this *RleEncoder) flush(buffer *bytes.Buffer) {
	lastBitPackedNum := int32(this.numBufferedValues)
	if this.repeatCount >= conf.RLE_MIN_REPEATED_NUM {
		this.writeRleRun()
	} else if this.numBufferedValues > 0 {
		this.clearBuffer()
		this.writeOrAppendBitPackedRun()
		this.endPreviousBitPackedRun(lastBitPackedNum)
	} else {
		this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM))
	}

	utils.WriteUnsignedVarInt(int32(this.byteCache.Len()), buffer)
	buffer.Write(this.byteCache.Bytes())
	this.reset()
}

func (this *RleEncoder) Flush(buffer *bytes.Buffer) {

	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		this.bitWidth = int(getIntMaxBitWidth(this.values_32))
		this.packer_32 = &bitpacking.IntPacker{BitWidth: int(this.bitWidth)}
		for _, v := range this.values_32 {
			this.encodeValue(int64(v))
		}
		break
	case (constant.INT64):
		this.bitWidth = int(getLongMaxBitWidth(this.values_64))
		this.packer_64 = &bitpacking.LongPacker{BitWidth: int(this.bitWidth)}
		for _, v := range this.values_64 {
			this.encodeValue(int64(v))
		}
		break
	default:
		break
	}
	this.flush(buffer)
}

func (this *RleEncoder) GetMaxByteSize() int64 {
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		len := len(this.values_32)
		if len == 0 {
			return 0
		}
		groupNum := (len/8+1)/63 + 1
		return int64(8 + groupNum*5 + len*4)
	case (constant.INT64):
		len := len(this.values_32)
		if len == 0 {
			return 0
		}
		groupNum := (len/8+1)/63 + 1
		return int64(8 + groupNum*5 + len*8)
	default:
		log.Error("invalid input dataType in plainEncoder. tsDataType: %d", this.tsDataType)
	}
	return -1
}

func (this *RleEncoder) GetOneItemMaxSize() int {
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		return 45
	case (constant.INT64):
		return 77
	default:
		log.Error("invalid input dataType in plainEncoder. tsDataType: %d", this.tsDataType)
	}
	return -1
}

func NewRleEncoder(tdt constant.TSDataType) *RleEncoder {
	return &RleEncoder{
		tsDataType:        tdt,
		encodeEndian:      1,
		byteCache:         bytes.NewBuffer([]byte{}),
		bufferedValues_32: make([]int32, 0),
		bufferedValues_64: make([]int64, 0),
		values_32:         make([]int32, 0),
		values_64:         make([]int64, 0),
	}
}
