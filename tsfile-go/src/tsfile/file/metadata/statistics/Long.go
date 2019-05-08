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
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Long struct {
	max     int64
	min     int64
	first   int64
	last    int64
	sum     float64
	isEmpty bool
}

func (s *Long) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadLong()
	s.max = reader.ReadLong()
	s.first = reader.ReadLong()
	s.last = reader.ReadLong()
	s.sum = reader.ReadDouble()
}

func (l *Long) SizeOfDaum() int {
	return 8
}

func (l *Long) GetMaxByte(tdt int16) []byte {
	return utils.Int64ToByte(l.max, 0)
}

func (l *Long) GetMinByte(tdt int16) []byte {
	return utils.Int64ToByte(l.min, 0)
}

func (l *Long) GetFirstByte(tdt int16) []byte {
	return utils.Int64ToByte(l.first, 0)
}

func (l *Long) GetLastByte(tdt int16) []byte {
	return utils.Int64ToByte(l.last, 0)
}

func (l *Long) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(l.sum, 0)
}

func (l *Long) UpdateStats(lValue interface{}) {
	value := lValue.(int64)
	if !l.isEmpty {
		l.InitializeStats(value, value, value, value, float64(value))
		l.isEmpty = true
	} else {
		l.UpdateValue(value, value, value, value, float64(value))
	}
}

func (l *Long) UpdateValue(max int64, min int64, first int64, last int64, sum float64) {
	if max > l.max {
		l.max = max
	}
	if min < l.min {
		l.min = min
	}
	l.sum += sum
	l.last = last
}

func (l *Long) InitializeStats(max int64, min int64, first int64, last int64, sum float64) {
	l.max = max
	l.min = min
	l.first = first
	l.last = last
	l.sum = sum
}

func (s *Long) GetSerializedSize() int {
	return 4*constant.LONG_LEN + constant.DOUBLE_LEN
}
