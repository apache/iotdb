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

package basic

import (
	_ "bytes"
	_ "encoding/binary"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read/datatype"
)

type PageDataReader struct {
	DataType     constant.TSDataType
	ValueDecoder decoder.Decoder
	TimeDecoder  decoder.Decoder
}

func (r *PageDataReader) Read(data []byte) {
	reader := utils.NewBytesReader(data)
	timeInputStreamLength := int(reader.ReadUnsignedVarInt())
	pos := reader.Pos()

	r.TimeDecoder.Init(data[pos : timeInputStreamLength+pos])
	r.ValueDecoder.Init(data[timeInputStreamLength+pos:])
}

func (r *PageDataReader) HasNext() bool {
	return r.TimeDecoder.HasNext() && r.ValueDecoder.HasNext()
}

func (r *PageDataReader) Next() (*datatype.TimeValuePair, error) {
	// TODO: catch errors
	return &datatype.TimeValuePair{Timestamp: r.TimeDecoder.Next().(int64), Value: r.ValueDecoder.Next()}, nil
}

func (r *PageDataReader) Skip() {
	r.Next()
}

func (r *PageDataReader) Close() {
}
