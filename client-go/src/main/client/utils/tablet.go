/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package utils

import (
	"bytes"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"sort"
)

type Tablet struct {
	DeviceId     string
	Measurements []string
	Values       []interface{}
	Timestamps   []int64
	Types        []int32
}

func (t *Tablet) GetRowNumber() int32 {
	return (int32)(len(t.Timestamps))
}

func (t *Tablet) GetDeviceId() string {
	return t.DeviceId
}

func (t *Tablet) GetMeasurements() []string {
	return t.Measurements
}

func (t *Tablet) GetBinaryTimestamps() []byte {
	buf := bytes.NewBuffer([]byte{})
	for i := 0; i < len(t.Timestamps); i++ {
		binary.Write(buf, binary.BigEndian, t.Timestamps[i])
	}
	return buf.Bytes()
}

func (t *Tablet) GetBinaryValues() []byte {
	buf := bytes.NewBuffer([]byte{})
	for i := 0; i < len(t.Types); i++ {
		switch t.Types[i] {
		case 0:
			binary.Write(buf, binary.BigEndian, t.Values[i].([]bool))
		case 1:
			tmp := t.Values[i].([]int32)
			binary.Write(buf, binary.BigEndian, &tmp)
		case 2:
			tmp := t.Values[i].([]int64)
			binary.Write(buf, binary.BigEndian, &tmp)
		case 3:
			tmp := t.Values[i].([]float32)
			binary.Write(buf, binary.BigEndian, &tmp)
		case 4:
			tmp := t.Values[i].([]float64)
			binary.Write(buf, binary.BigEndian, &tmp)
		case 5:
			values := t.Values[i].([]string)
			for index := range values {
				tmp := (int32)(len(values[index]))
				binary.Write(buf, binary.BigEndian, &tmp)
				buf.WriteString(values[index])
			}

		}
	}
	return buf.Bytes()
}

func (t *Tablet) GetTypes() []int32 {
	return t.Types
}

func (t *Tablet) SortTablet() {
	var timeIndexs = make(map[int64]int, t.GetRowNumber())
	for index := range t.Timestamps {
		timeIndexs[t.Timestamps[index]] = index
	}
	var keys []int64
	for timeValue := range timeIndexs {
		keys = append(keys, timeValue)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	t.Timestamps = keys
	for index := range t.Values {
		sortValue := sortList(t.Values[index], t.Types[index], timeIndexs, t.Timestamps)
		if sortValue != nil {
			t.Values[index] = sortValue
		} else {
			log.Error("unsupported data type ", t.Types[index])
		}
	}
}

func sortList(valueList interface{}, dataType int32, timeIndexs map[int64]int, timeStamps []int64) interface{} {
	switch dataType {
	case 0:
		boolValues := valueList.([]bool)
		sortedValues := make([]bool, len(boolValues))
		for index := range sortedValues {
			sortedValues[index] = boolValues[timeIndexs[timeStamps[index]]]
		}
		return sortedValues
	case 1:
		intValues := valueList.([]int32)
		sortedValues := make([]int32, len(intValues))
		for index := range sortedValues {
			sortedValues[index] = intValues[timeIndexs[timeStamps[index]]]
		}
		return sortedValues
	case 2:
		longValues := valueList.([]int64)
		sortedValues := make([]int64, len(longValues))
		for index := range sortedValues {
			sortedValues[index] = longValues[timeIndexs[timeStamps[index]]]
		}
		return sortedValues
	case 3:
		floatValues := valueList.([]float32)
		sortedValues := make([]float32, len(floatValues))
		for index := range sortedValues {
			sortedValues[index] = floatValues[timeIndexs[timeStamps[index]]]
		}
		return sortedValues
	case 4:
		doubleValues := valueList.([]float64)
		sortedValues := make([]float64, len(doubleValues))
		for index := range sortedValues {
			sortedValues[index] = doubleValues[timeIndexs[timeStamps[index]]]
		}
		return sortedValues
	case 5:
		stringValues := valueList.([]string)
		sortedValues := make([]string, len(stringValues))
		for index := range sortedValues {
			sortedValues[index] = stringValues[timeIndexs[timeStamps[index]]]
		}
		return sortedValues
	}
	return nil
}
