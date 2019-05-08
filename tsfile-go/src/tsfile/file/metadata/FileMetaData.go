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
	_ "encoding/binary"
	_ "log"
	"tsfile/common/utils"
)

type FileMetaData struct {
	currentVersion                   int
	createdBy                        string
	firstTimeSeriesMetadataOffset    int64 //相对于file metadata开头位置 的offset
	lastTimeSeriesMetadataOffset     int64 //相对于file metadata开头位置 的offset
	firstTsDeltaObjectMetadataOffset int64 //相对于file metadata开头位置 的offset
	lastTsDeltaObjectMetadataOffset  int64 //相对于file metadata开头位置 的offset

	deviceMap             map[string]*DeviceMetaData
	timeSeriesMetadataMap map[string]*TimeSeriesMetaData
}

func (f *FileMetaData) TimeSeriesMetadataMap() map[string]*TimeSeriesMetaData {
	return f.timeSeriesMetadataMap
}

func (f *FileMetaData) DeviceMap() map[string]*DeviceMetaData {
	return f.deviceMap
}

func (f *FileMetaData) Deserialize(metadata []byte) {
	reader := utils.NewBytesReader(metadata)

	f.deviceMap = make(map[string]*DeviceMetaData)
	if size := int(reader.ReadInt()); size > 0 {
		for i := 0; i < size; i++ {
			key := reader.ReadString()

			value := new(DeviceMetaData)
			value.Deserialize(reader)

			f.deviceMap[key] = value
		}
	}

	f.timeSeriesMetadataMap = make(map[string]*TimeSeriesMetaData)
	if size := int(reader.ReadInt()); size > 0 {
		for i := 0; i < size; i++ {
			value := new(TimeSeriesMetaData)
			value.Deserialize(reader)

			f.timeSeriesMetadataMap[value.GetSensor()] = value
		}
	}

	f.currentVersion = int(reader.ReadInt())
	if reader.ReadBool() {
		f.createdBy = reader.ReadString()
	}
	f.firstTimeSeriesMetadataOffset = reader.ReadLong()
	f.lastTimeSeriesMetadataOffset = reader.ReadLong()
	f.firstTsDeltaObjectMetadataOffset = reader.ReadLong()
	f.lastTsDeltaObjectMetadataOffset = reader.ReadLong()
}

func (f *FileMetaData) GetCurrentVersion() int {
	return f.currentVersion
}

func (t *FileMetaData) SerializeTo(buf *bytes.Buffer) int {
	var byteLen int
	if t.deviceMap == nil {
		n, _ := buf.Write(utils.Int32ToByte(0, 0))
		byteLen += n
	} else {
		n := len(t.deviceMap)
		d1, _ := buf.Write(utils.Int32ToByte(int32(n), 0))
		byteLen += d1

		for k, v := range t.deviceMap {
			// write string tsDeviceMetaData key
			d2, _ := buf.Write(utils.Int32ToByte(int32(len(k)), 0))
			byteLen += d2
			d3, _ := buf.Write([]byte(k))
			byteLen += d3
			// tsDeviceMetaData SerializeTo
			byteLen += v.SerializeTo(buf)
			// log.Info("v: %s", v)
		}
	}
	if t.timeSeriesMetadataMap == nil {
		e1, _ := buf.Write(utils.Int32ToByte(0, 0))
		byteLen += e1
	} else {
		e2, _ := buf.Write(utils.Int32ToByte(int32(len(t.timeSeriesMetadataMap)), 0))
		byteLen += e2
		for _, vv := range t.timeSeriesMetadataMap {
			// timeSeriesMetaData SerializeTo
			byteLen += vv.Serialize(buf)
			// log.Info("vv: %s", vv)
		}
	}
	f1, _ := buf.Write(utils.Int32ToByte(int32(t.currentVersion), 0))
	byteLen += f1
	if t.createdBy == "" {
		// write flag for t.createBy
		f2, _ := buf.Write(utils.BoolToByte(false, 0))
		byteLen += f2
	} else {
		// write flag for t.createBy
		f3, _ := buf.Write(utils.BoolToByte(true, 0))
		byteLen += f3
		// write string t.createBy
		f4, _ := buf.Write(utils.Int32ToByte(int32(len(t.createdBy)), 0))
		byteLen += f4
		f5, _ := buf.Write([]byte(t.createdBy))
		byteLen += f5
	}

	off1, _ := buf.Write(utils.Int64ToByte(t.firstTimeSeriesMetadataOffset, 0))
	byteLen += off1
	off2, _ := buf.Write(utils.Int64ToByte(t.lastTimeSeriesMetadataOffset, 0))
	byteLen += off2
	off3, _ := buf.Write(utils.Int64ToByte(t.firstTsDeltaObjectMetadataOffset, 0))
	byteLen += off3
	off4, _ := buf.Write(utils.Int64ToByte(t.lastTsDeltaObjectMetadataOffset, 0))
	byteLen += off4

	return byteLen
}

func NewTsFileMetaData(tdmd map[string]*DeviceMetaData, tss map[string]*TimeSeriesMetaData, version int) (*FileMetaData, error) {

	return &FileMetaData{
		deviceMap:             tdmd,
		timeSeriesMetadataMap: tss,
		currentVersion:        version,
		createdBy:             "",
	}, nil
}
