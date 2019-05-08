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
	"tsfile/common/utils"
)

type Binary struct {
	//max   string
	//min   string
	//first string
	//last  string
	//sum   float64 //meaningless
	max     []byte
	min     []byte
	first   []byte
	last    []byte
	sum     float64 //meaningless
	isEmpty bool
}

func (s *Binary) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadStringBinary()
	s.max = reader.ReadStringBinary()
	s.first = reader.ReadStringBinary()
	s.last = reader.ReadStringBinary()
	s.sum = reader.ReadDouble()
}

func (b *Binary) SizeOfDaum() int {
	return -1
}

func (b *Binary) GetMaxByte(tdt int16) []byte {
	return []byte(b.max)
}

func (b *Binary) GetMinByte(tdt int16) []byte {
	return []byte(b.min)
}

func (b *Binary) GetFirstByte(tdt int16) []byte {
	return []byte(b.first)
}

func (b *Binary) GetLastByte(tdt int16) []byte {
	return []byte(b.last)
}

func (b *Binary) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(b.sum, 0)
}

func (b *Binary) UpdateStats(fValue interface{}) {
	value := []byte(fValue.(string))
	if !b.isEmpty {
		b.InitializeStats(value, value, value, value, 0)
		b.isEmpty = true
	} else {
		b.UpdateValue(value, value, value, value, 0)
	}
}

func (b *Binary) UpdateValue(max []byte, min []byte, first []byte, last []byte, sum float64) {
	// todo compare two []byte
	retMax := bytes.Compare(max, b.max)
	if retMax > 0 {
		b.max = max
	}
	retMin := bytes.Compare(min, b.min)
	if retMin < 0 {
		b.min = min
	}

	b.last = last
}

func (b *Binary) InitializeStats(max []byte, min []byte, first []byte, last []byte, sum float64) {
	b.max = max
	b.min = min
	b.first = first
	b.last = last
	b.sum = sum
}

func (s *Binary) GetSerializedSize() int {
	return 4*4 + len(s.max) + len(s.min) + len(s.first) + len(s.last)
}
