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

package sensorDescriptor

/**
 * @Package Name: measurementDescriptor
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-24 下午7:38
 * @Description:
 */

import (
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/compress"
	"tsfile/encoding/encoder"
)

type SensorDescriptor struct {
	sensorId           string
	tsDataType         int16
	tsEncoding         int16
	timeCount          int
	compressor         *compress.Encompress
	tsCompresstionType int16
}

func (s *SensorDescriptor) GetTimeCount() int {
	return s.timeCount
}

func (s *SensorDescriptor) SetTimeCount(count int) {
	s.timeCount = count
	return
}

func (s *SensorDescriptor) GetSensorId() string {
	return s.sensorId
}

func (s *SensorDescriptor) GetTsDataType() int16 {
	return s.tsDataType
}

func (s *SensorDescriptor) GetTsEncoding() int16 {
	return s.tsEncoding
}

func (s *SensorDescriptor) GetCompresstionType() int16 {
	return s.tsCompresstionType
}

// the return type should be Compressor, after finished Compressor we should modify it.
func (s *SensorDescriptor) GetCompressor() *compress.Encompress {
	return s.compressor
}

func (s *SensorDescriptor) GetTimeEncoder() encoder.Encoder {
	return encoder.GetEncoder(int16(constant.GetEncodingByName(conf.TimeSeriesEncoder)), int16(constant.INT64))
}

func (s *SensorDescriptor) GetValueEncoder() encoder.Encoder {
	return encoder.GetEncoder(s.GetTsEncoding(), s.GetTsDataType())
}

func (s *SensorDescriptor) Close() bool {
	return true
}

func New(sId string, tdt constant.TSDataType, te constant.TSEncoding) (*SensorDescriptor, error) {
	// init compressor
	enCompressor := new(compress.Encompress)
	return &SensorDescriptor{
		sensorId:           sId,
		tsDataType:         int16(tdt),
		tsEncoding:         int16(te),
		compressor:         enCompressor,
		tsCompresstionType: int16(constant.UNCOMPRESSED),
		timeCount:          -1,
	}, nil
}

func NewWithCompress(sId string, tdt constant.TSDataType, te constant.TSEncoding, tct constant.CompressionType) (*SensorDescriptor, error) {
	// init compressor
	enCompressor := new(compress.Encompress)
	return &SensorDescriptor{
		sensorId:           sId,
		tsDataType:         int16(tdt),
		tsEncoding:         int16(te),
		compressor:         enCompressor,
		tsCompresstionType: int16(tct),
		timeCount:          -1,
	}, nil
}
