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

package statistics

import (
	"bytes"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Statistics interface {
	Deserialize(reader *utils.FileReader)
	GetSerializedSize() int
	GetMaxByte(tdt int16) []byte
	GetMinByte(tdt int16) []byte
	GetFirstByte(tdt int16) []byte
	GetLastByte(tdt int16) []byte
	GetSumByte(tdt int16) []byte
	SizeOfDaum() int
	UpdateStats(value interface{})
}

func Deserialize(reader *utils.FileReader, dataType constant.TSDataType) Statistics {
	var statistics Statistics

	switch dataType {
	case constant.BOOLEAN:
		statistics = new(Boolean)
	case constant.INT32:
		statistics = new(Integer)
	case constant.INT64:
		statistics = new(Long)
	case constant.FLOAT:
		statistics = new(Float)
	case constant.DOUBLE:
		statistics = new(Double)
	case constant.TEXT:
		statistics = new(Binary)
	default:
		panic("Statistics unknown dataType: " + strconv.Itoa(int(dataType)))
	}

	statistics.Deserialize(reader)

	return statistics
}

func GetStatsByType(tsDataType int16) Statistics {
	var statistics Statistics
	switch constant.TSDataType(tsDataType) {
	case constant.BOOLEAN:
		statistics = new(Boolean)
	case constant.INT32:
		statistics = new(Integer)
	case constant.INT64:
		statistics = new(Long)
	case constant.FLOAT:
		statistics = new(Float)
	case constant.DOUBLE:
		statistics = new(Double)
	case constant.TEXT:
		statistics = new(Binary)
	default:
		panic("Statistics unknown dataType: " + strconv.Itoa(int(tsDataType)))
	}
	return statistics
}

func Serialize(s Statistics, buffer *bytes.Buffer, tsDataType int16) int {
	var length int
	if s.SizeOfDaum() == 0 {
		return 0
	} else if s.SizeOfDaum() != -1 {
		buffer.Write(s.GetMaxByte(tsDataType))
		buffer.Write(s.GetMinByte(tsDataType))
		buffer.Write(s.GetFirstByte(tsDataType))
		buffer.Write(s.GetLastByte(tsDataType))
		buffer.Write(s.GetSumByte(tsDataType))
		length = s.SizeOfDaum()*4 + 8
	} else {
		maxData := s.GetMaxByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(maxData)), 0))
		maxLen, _ := buffer.Write(maxData)
		length += maxLen
		minData := s.GetMinByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(minData)), 0))
		minLen, _ := buffer.Write(minData)
		length += minLen
		firstData := s.GetFirstByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(firstData)), 0))
		firstLen, _ := buffer.Write(firstData)
		length += firstLen
		lastData := s.GetLastByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(lastData)), 0))
		lastLen, _ := buffer.Write(lastData)
		length += lastLen
		sumData := s.GetSumByte(tsDataType)
		//buffer.Write(utils.Int32ToByte(int32(len(sumData)), 0))
		sumLen, _ := buffer.Write(sumData)
		length += sumLen
		length = length + 4*4 + 8
	}
	return length
}
