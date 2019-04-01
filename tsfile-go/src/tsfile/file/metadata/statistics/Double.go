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

type Double struct {
	max     float64
	min     float64
	first   float64
	last    float64
	sum     float64
	isEmpty bool
}

func (s *Double) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadDouble()
	s.max = reader.ReadDouble()
	s.first = reader.ReadDouble()
	s.last = reader.ReadDouble()
	s.sum = reader.ReadDouble()
}

func (d *Double) SizeOfDaum() int {
	return 4
}

func (d *Double) GetMaxByte(tdt int16) []byte {
	return utils.Float64ToByte(d.max, 0)
}

func (d *Double) GetMinByte(tdt int16) []byte {
	return utils.Float64ToByte(d.min, 0)
}

func (d *Double) GetFirstByte(tdt int16) []byte {
	return utils.Float64ToByte(d.first, 0)
}

func (d *Double) GetLastByte(tdt int16) []byte {
	return utils.Float64ToByte(d.last, 0)
}

func (d *Double) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(d.sum, 0)
}

func (d *Double) UpdateStats(dValue interface{}) {
	value := dValue.(float64)
	if !d.isEmpty {
		d.InitializeStats(value, value, value, value, value)
		d.isEmpty = true
	} else {
		d.UpdateValue(value, value, value, value, value)
	}
}

func (d *Double) UpdateValue(max float64, min float64, first float64, last float64, sum float64) {
	if max > d.max {
		d.max = max
	}
	if min < d.min {
		d.min = min
	}
	d.sum += sum
	d.last = last
}

func (d *Double) InitializeStats(max float64, min float64, first float64, last float64, sum float64) {
	d.max = max
	d.min = min
	d.first = first
	d.last = last
	d.sum = sum
}

func (s *Double) GetSerializedSize() int {
	return constant.DOUBLE_LEN * 5
}
