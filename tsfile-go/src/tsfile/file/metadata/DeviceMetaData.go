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

type DeviceMetaData struct {
	startTime                 int64
	endTime                   int64
	serializedSize            int
	rowGroupMetadataSli       []*RowGroupMetaData
	sizeOfRowGroupMetaDataSli int
}

func (f *DeviceMetaData) Deserialize(reader *utils.BytesReader) {
	f.startTime = reader.ReadLong()
	f.endTime = reader.ReadLong()

	size := int(reader.ReadInt())
	if size > 0 {
		f.rowGroupMetadataSli = make([]*RowGroupMetaData, 0)
		for i := 0; i < size; i++ {
			rowGroupMetaData := new(RowGroupMetaData)
			rowGroupMetaData.Deserialize(reader)

			f.rowGroupMetadataSli = append(f.rowGroupMetadataSli, rowGroupMetaData)
		}
	}
}

func (f *DeviceMetaData) GetSerializedSize() int {
	f.serializedSize = 2*constant.LONG_LEN + constant.INT_LEN
	if len(f.rowGroupMetadataSli) > 0 {
		// iterate list
		for _, v := range f.rowGroupMetadataSli {
			f.serializedSize += v.GetSerializedSize()
		}
	}

	return f.serializedSize
}

func (t *DeviceMetaData) AddRowGroupMetaData(rgmd *RowGroupMetaData) {
	if len(t.rowGroupMetadataSli) == 0 {
		t.rowGroupMetadataSli = make([]*RowGroupMetaData, 0)
	}
	t.rowGroupMetadataSli = append(t.rowGroupMetadataSli, rgmd)
	t.sizeOfRowGroupMetaDataSli += 1
	t.serializedSize += rgmd.GetserializedSize()
}

func (t *DeviceMetaData) GetRowGroups() []*RowGroupMetaData {
	return t.rowGroupMetadataSli
}

func (t *DeviceMetaData) GetStartTime() int64 {
	return t.startTime
}

func (t *DeviceMetaData) SetStartTime(time int64) {
	t.startTime = time
}

func (t *DeviceMetaData) GetEndTime() int64 {
	return t.endTime
}

func (t *DeviceMetaData) SetEndTime(time int64) {
	t.endTime = time
}

func (t *DeviceMetaData) SerializeTo(buf *bytes.Buffer) int {
	if t.sizeOfRowGroupMetaDataSli != len(t.rowGroupMetadataSli) {
		t.ReCalculateSerializedSize()
	}
	var byteLen int

	n1, _ := buf.Write(utils.Int64ToByte(t.startTime, 0))
	byteLen += n1
	n2, _ := buf.Write(utils.Int64ToByte(t.endTime, 0))
	byteLen += n2

	if len(t.rowGroupMetadataSli) == 0 {
		n3, _ := buf.Write(utils.Int32ToByte(0, 0))
		byteLen += n3
	} else {
		n4, _ := buf.Write(utils.Int32ToByte(int32(len(t.rowGroupMetadataSli)), 0))
		byteLen += n4
		for _, v := range t.rowGroupMetadataSli {
			// serialize RowGroupMetaData
			byteLen += v.SerializeTo(buf)
		}
	}
	return byteLen
}

func (t *DeviceMetaData) ReCalculateSerializedSize() {
	t.serializedSize = 2*8 + 1*4
	if t.rowGroupMetadataSli != nil {
		for _, v := range t.rowGroupMetadataSli {
			t.serializedSize += v.GetserializedSize()
		}
		t.sizeOfRowGroupMetaDataSli = len(t.rowGroupMetadataSli)
	}
	t.sizeOfRowGroupMetaDataSli = 0
}

func NewTimeDeviceMetaData() (*DeviceMetaData, error) {

	return &DeviceMetaData{
		rowGroupMetadataSli: make([]*RowGroupMetaData, 0),
		serializedSize:      2*8 + 1*4,
	}, nil
}
