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
	"encoding/binary"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type PlainDecoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType
	reader     *utils.BytesReader
}

func (d *PlainDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *PlainDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *PlainDecoder) Next() interface{} {
	switch {
	case d.dataType == constant.BOOLEAN:
		return d.reader.ReadBool()
	case d.dataType == constant.INT32:
		result := d.reader.ReadSlice(4)
		return int32(binary.LittleEndian.Uint32(result))
	case d.dataType == constant.INT64:
		result := d.reader.ReadSlice(8)
		return int64(binary.LittleEndian.Uint64(result))
	case d.dataType == constant.FLOAT:
		return d.reader.ReadFloat()
	case d.dataType == constant.DOUBLE:
		return d.reader.ReadDouble()
	case d.dataType == constant.TEXT:
		len_bytes := d.reader.ReadSlice(4)
		len := int32(binary.LittleEndian.Uint32(len_bytes))
		return string(d.reader.ReadSlice(int(len)))
	default:
		panic("ReadValue not supported: " + strconv.Itoa(int(d.dataType)))
	}
}
