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

package fileSchema

/**
 * @Package Name: fileSchema
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-28 下午8:55
 * @Description:
 */

import (
	"tsfile/file/metadata"
	"tsfile/timeseries/write/sensorDescriptor"
)

type FileSchema struct {
	sensorDescriptorMap        map[string]*sensorDescriptor.SensorDescriptor
	additionalProperties       map[string]string
	currentMaxByteSizeInOneRow int
	tsMetaData                 map[string]*metadata.TimeSeriesMetaData
	sensorDataTypeMap          map[string]int16
}

func (f *FileSchema) AddTimeSeriesMetaData(sensorId string, tsDataType int16) {
	ts, _ := metadata.NewTimeSeriesMetaData(sensorId, tsDataType)
	f.tsMetaData[sensorId] = ts
}

func (f *FileSchema) GetTimeSeriesMetaDatas() map[string]*metadata.TimeSeriesMetaData {
	return f.tsMetaData
}

func (f *FileSchema) GetSensorDescriptiorMap() map[string]*sensorDescriptor.SensorDescriptor {
	return f.sensorDescriptorMap
}

func (f *FileSchema) GetCurrentRowMaxSize() int {
	return f.currentMaxByteSizeInOneRow
}

func (f *FileSchema) enlargeMaxByteSizeInOneRow(byteSize int) {
	f.currentMaxByteSizeInOneRow += byteSize
}

func (f *FileSchema) indexSensorDataType(sensorId string, tsDataType int16) {
	f.sensorDataTypeMap[sensorId] = tsDataType
}

func (f *FileSchema) Registermeasurement(sd *sensorDescriptor.SensorDescriptor) bool {
	f.sensorDescriptorMap[sd.GetSensorId()] = sd
	f.indexSensorDataType(sd.GetSensorId(), sd.GetTsDataType())
	f.AddTimeSeriesMetaData(sd.GetSensorId(), sd.GetTsDataType())
	// todo fileschema.java line:178
	if sd.GetTimeEncoder() != nil && sd.GetValueEncoder() != nil {
		f.enlargeMaxByteSizeInOneRow(sd.GetTimeEncoder().GetOneItemMaxSize() + sd.GetValueEncoder().GetOneItemMaxSize())
	}
	return true
}

func New() (*FileSchema, error) {
	return &FileSchema{
		sensorDescriptorMap:  make(map[string]*sensorDescriptor.SensorDescriptor),
		additionalProperties: make(map[string]string),
		tsMetaData:           make(map[string]*metadata.TimeSeriesMetaData),
		sensorDataTypeMap:    make(map[string]int16),
	}, nil
}
