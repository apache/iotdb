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

package metadata

import (
	"bytes"
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type TimeSeriesMetaData struct {
	sensor   string
	dataType constant.TSDataType
}

func (t *TimeSeriesMetaData) DataType() constant.TSDataType {
	return t.dataType
}

func (f *TimeSeriesMetaData) Deserialize(reader *utils.BytesReader) {
	if reader.ReadBool() {
		f.sensor = reader.ReadString()
	}

	if reader.ReadBool() {
		f.dataType = constant.TSDataType(reader.ReadShort())
	}
}

func (f *TimeSeriesMetaData) GetSensor() string {
	return f.sensor
}

func (t *TimeSeriesMetaData) Serialize(buf *bytes.Buffer) int {
	var byteLen int
	if t.sensor == "" {
		n1, _ := buf.Write(utils.BoolToByte(false, 0))
		byteLen += n1
	} else {
		n2, _ := buf.Write(utils.BoolToByte(true, 0))
		byteLen += n2

		n3, _ := buf.Write(utils.Int32ToByte(int32(len(t.sensor)), 0))
		byteLen += n3
		n4, _ := buf.Write([]byte(t.sensor))
		byteLen += n4
	}

	if t.dataType >= 0 && t.dataType <= 9 { // not empty
		n5, _ := buf.Write(utils.BoolToByte(true, 0))
		byteLen += n5

		n6, _ := buf.Write(utils.Int16ToByte(int16(t.dataType), 0))
		byteLen += n6
	} else {
		n7, _ := buf.Write(utils.BoolToByte(false, 0))
		byteLen += n7
	}

	return byteLen
}

func NewTimeSeriesMetaData(sid string, tdt int16) (*TimeSeriesMetaData, error) {

	return &TimeSeriesMetaData{
		sensor:   sid,
		dataType: constant.TSDataType(tdt),
	}, nil
}
