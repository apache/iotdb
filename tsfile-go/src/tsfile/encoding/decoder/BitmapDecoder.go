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
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type BitmapDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader *utils.BytesReader

	// how many bytes for all encoded data
	length int
	// number of encoded data
	number int
	// number of data left for reading in current buffer
	currentCount int
	// decoder reads all bitmap index from byteCache and save in
	buffer map[int32][]byte
}

func (d *BitmapDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)

	d.length = 0
	d.number = 0
	d.currentCount = 0
}

func (d *BitmapDecoder) Next() interface{} {
	if d.currentCount == 0 {
		// reset
		d.length = 0
		d.number = 0
		d.buffer = make(map[int32][]byte)

		// getLengthAndNumber
		d.length = int(d.reader.ReadUnsignedVarInt())
		d.number = int(d.reader.ReadUnsignedVarInt())

		d.readPackage()
	}

	var result int32 = 0
	index := (d.number - d.currentCount) / 8
	offset := 7 - ((d.number - d.currentCount) % 8)
	for k, v := range d.buffer {
		if v[index]&(1<<uint(offset)) != 0 {
			result = k
			break
		}
	}

	d.currentCount--

	return result
}

func (d *BitmapDecoder) readPackage() {
	packageReader := utils.NewBytesReader(d.reader.ReadSlice(int(d.length)))

	len := (d.number + 7) / 8
	for packageReader.Len() > 0 {
		value := packageReader.ReadUnsignedVarInt()
		data := packageReader.ReadBytes(len)

		d.buffer[value] = data
	}

	d.currentCount = d.number
}
