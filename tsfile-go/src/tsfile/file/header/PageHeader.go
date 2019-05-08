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

package header

import (
	_ "bufio"
	"bytes"
	_ "log"
	_ "os"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/file/metadata/statistics"
)

type PageHeader struct {
	uncompressedSize int32
	compressedSize   int32
	numberOfValues   int32
	max_timestamp    int64
	min_timestamp    int64
	statistics       statistics.Statistics
	serializedSize   int32
}

func (p *PageHeader) Min_timestamp() int64 {
	return p.min_timestamp
}

func (p *PageHeader) Max_timestamp() int64 {
	return p.max_timestamp
}

func (h *PageHeader) Deserialize(reader *utils.FileReader, dataType constant.TSDataType) {
	h.uncompressedSize = reader.ReadInt()
	h.compressedSize = reader.ReadInt()
	h.numberOfValues = reader.ReadInt()
	h.max_timestamp = reader.ReadLong()
	h.min_timestamp = reader.ReadLong()
	h.statistics = statistics.Deserialize(reader, dataType)

	h.serializedSize = int32(3*constant.INT_LEN + 2*constant.LONG_LEN + h.statistics.GetSerializedSize())
}

func (h *PageHeader) GetUncompressedSize() int32 {
	return h.uncompressedSize
}

func (h *PageHeader) GetCompressedSize() int32 {
	return h.compressedSize
}

func (h *PageHeader) GetNumberOfValues() int32 {
	return h.numberOfValues
}

func (h *PageHeader) GetSerializedSize() int32 {
	return h.serializedSize
}

func (p *PageHeader) PageHeaderToMemory(buffer *bytes.Buffer, tsDataType int16) int32 {
	// write header to buffer
	buffer.Write(utils.Int32ToByte(p.uncompressedSize, 0))
	buffer.Write(utils.Int32ToByte(p.compressedSize, 0))
	buffer.Write(utils.Int32ToByte(p.numberOfValues, 0))
	buffer.Write(utils.Int64ToByte(p.max_timestamp, 0))
	buffer.Write(utils.Int64ToByte(p.min_timestamp, 0))
	statistics.Serialize(p.statistics, buffer, tsDataType)
	return p.serializedSize
}

func CalculatePageHeaderSize(tsDataType int16) int {
	pHeaderSize := 3*4 + 2*8
	// statisticsSize := statistics.GetStatistics(tsDataType).GetserializedSize(tsDataType)
	statisticsSize := statistics.GetStatsByType(tsDataType).GetSerializedSize()
	return pHeaderSize + statisticsSize
}

func NewPageHeader(ucs int32, cs int32, nov int32, sts statistics.Statistics, max_t int64, min_t int64, tsDataType int16) (*PageHeader, error) {
	ss := 3*4 + 2*8 + sts.GetSerializedSize()
	return &PageHeader{
		uncompressedSize: ucs,
		compressedSize:   cs,
		numberOfValues:   nov,
		max_timestamp:    max_t,
		min_timestamp:    min_t,
		statistics:       sts,
		serializedSize:   int32(ss),
	}, nil
}
