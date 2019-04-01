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

type Integer struct {
	max     int32
	min     int32
	first   int32
	last    int32
	sum     float64
	isEmpty bool
}

func (s *Integer) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadInt()
	s.max = reader.ReadInt()
	s.first = reader.ReadInt()
	s.last = reader.ReadInt()
	s.sum = reader.ReadDouble()
}

func (i *Integer) SizeOfDaum() int {
	return 4
}

func (i *Integer) GetMaxByte(tdt int16) []byte {
	return utils.Int32ToByte(i.max, 0)
}

func (i *Integer) GetMinByte(tdt int16) []byte {
	return utils.Int32ToByte(i.min, 0)
}

func (i *Integer) GetFirstByte(tdt int16) []byte {
	return utils.Int32ToByte(i.first, 0)
}

func (i *Integer) GetLastByte(tdt int16) []byte {
	return utils.Int32ToByte(i.last, 0)
}

func (i *Integer) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(i.sum, 0)
}

func (i *Integer) UpdateStats(iValue interface{}) {
	value := iValue.(int32)
	if !i.isEmpty {
		i.InitializeStats(value, value, value, value, float64(value))
		i.isEmpty = true
	} else {
		i.UpdateValue(value, value, value, value, float64(value))
	}
}

func (i *Integer) UpdateValue(max int32, min int32, first int32, last int32, sum float64) {
	if max > i.max {
		i.max = max
	}
	if min < i.min {
		i.min = min
	}
	i.sum += sum
	i.last = last
}

func (i *Integer) InitializeStats(max int32, min int32, first int32, last int32, sum float64) {
	i.max = max
	i.min = min
	i.first = first
	i.last = last
	i.sum = sum
}

func (s *Integer) GetSerializedSize() int {
	return 4*constant.INT_LEN + constant.DOUBLE_LEN
}
