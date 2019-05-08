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

type Float struct {
	max     float32
	min     float32
	first   float32
	last    float32
	sum     float64
	isEmpty bool
}

func (s *Float) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadFloat()
	s.max = reader.ReadFloat()
	s.first = reader.ReadFloat()
	s.last = reader.ReadFloat()
	s.sum = reader.ReadDouble()
}

func (f *Float) SizeOfDaum() int {
	return 4
}

func (f *Float) GetMaxByte(tdt int16) []byte {
	return utils.Float32ToByte(f.max, 0)
}

func (f *Float) GetMinByte(tdt int16) []byte {
	return utils.Float32ToByte(f.min, 0)
}

func (f *Float) GetFirstByte(tdt int16) []byte {
	return utils.Float32ToByte(f.first, 0)
}

func (f *Float) GetLastByte(tdt int16) []byte {
	return utils.Float32ToByte(f.last, 0)
}

func (f *Float) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(f.sum, 0)
}

func (f *Float) UpdateStats(fValue interface{}) {
	value := fValue.(float32)
	if !f.isEmpty {
		f.InitializeStats(value, value, value, value, float64(value))
		f.isEmpty = true
	} else {
		f.UpdateValue(value, value, value, value, float64(value))
	}
}

func (f *Float) UpdateValue(max float32, min float32, first float32, last float32, sum float64) {
	if max > f.max {
		f.max = max
	}
	if min < f.min {
		f.min = min
	}
	f.sum += sum
	f.last = last
}

func (f *Float) InitializeStats(max float32, min float32, first float32, last float32, sum float64) {
	f.max = max
	f.min = min
	f.first = first
	f.last = last
	f.sum = sum
}

func (s *Float) GetSerializedSize() int {
	return 4*constant.FLOAT_LEN + constant.DOUBLE_LEN
}
