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

package tsFileWriter

/**
 * @Package Name: valueWriter
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-31 下午4:51
 * @Description:
 */

import (
	"bytes"
	"tsfile/common/conf"
	"tsfile/common/utils"
	"tsfile/encoding/encoder"
	"tsfile/timeseries/write/sensorDescriptor"
)

type ValueWriter struct {
	// time
	timeEncoder  encoder.Encoder //interface{}
	valueEncoder encoder.Encoder //interface{}
	timeBuf      *bytes.Buffer
	valueBuf     *bytes.Buffer
	desc         *sensorDescriptor.SensorDescriptor
	//buf := bytes.NewBuffer([]byte{})
}

func (v *ValueWriter) GetCurrentMemSize() int {
	return v.timeBuf.Len() + v.valueBuf.Len() +
		int(v.timeEncoder.GetMaxByteSize()) + int(v.valueEncoder.GetMaxByteSize())
}

func (v *ValueWriter) PrepareEndWriteOnePage() {
	v.timeEncoder.Flush(v.timeBuf)
	v.valueEncoder.Flush(v.valueBuf)
}

func (v *ValueWriter) GetByteBuffer() *bytes.Buffer {
	v.PrepareEndWriteOnePage()
	encodeBuffer := bytes.NewBuffer([]byte{})
	var timeLen int32 = int32(v.timeBuf.Len())

	// write timeBuf size
	utils.WriteUnsignedVarInt(timeLen, encodeBuffer)

	//声明一个空的slice,容量为timebuf的长度
	timeSlice := make([]byte, timeLen)
	//把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
	v.timeBuf.Read(timeSlice)
	encodeBuffer.Write(timeSlice)

	//声明一个空的value slice,容量为valuebuf的长度
	valueSlice := make([]byte, v.valueBuf.Len())
	//把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
	v.valueBuf.Read(valueSlice)
	encodeBuffer.Write(valueSlice)

	return encodeBuffer
}

// write with encoder
func (v *ValueWriter) Write(t int64, tdt int16, data *DataPoint, valueCount int) {
	v.timeEncoder.Encode(t, v.timeBuf)
	switch tdt {
	case 0, 1, 2, 3, 4, 5:
		v.valueEncoder.Encode(data.value, v.valueBuf)
	case 6:
		//fixed_len_byte_array
	case 7:
		//enums
	case 8:
		//bigdecimal
	default:
		// int32
	}
}

// write without encoder
func (v *ValueWriter) WriteWithoutEnc(t int64, tdt int16, value interface{}, valueCount int) {
	var timeByteData []byte
	var valueByteData []byte
	switch tdt {
	case 0:
		// bool
		if data, ok := value.(bool); ok {
			// encode
			valueByteData = utils.BoolToByte(data, 1)
		}
	case 1:
		//int32
		if data, ok := value.(int32); ok {
			valueByteData = utils.Int32ToByte(data, 1)
		}
	case 2:
		//int64
		if data, ok := value.(int64); ok {
			valueByteData = utils.Int64ToByte(data, 1)
		}

	case 3:
		//float
		if data, ok := value.(float32); ok {
			valueByteData = utils.Float32ToByte(data, 1)
		}
	case 4:
		//double , float64 in golang as double in c
		if data, ok := value.(float64); ok {
			valueByteData = utils.Float64ToByte(data, 1)
		}
	case 5:
		//text
		if data, ok := value.(string); ok {
			valueByteData = []byte(data)
		}
	case 6:
		//fixed_len_byte_array
	case 7:
		//enums
	case 8:
		//bigdecimal
	default:
		// int32
	}
	// write time to byteBuffer
	timeByteData = utils.Int64ToByte(t, 1)

	// write to byteBuffer
	if valueCount == 0 {
		aa := []byte{24}
		v.timeBuf.Write(aa)
		//s.timeBuf.Write(utils.BoolToByte(true))
		//v.timeBuf.Write(timeByteData)
		//v.timeBuf.Write(timeByteData)
		//v.timeBuf.Write(timeByteData)
		//s.desc.SetTimeCount(encodeCount + 1)
	}
	v.timeBuf.Write(timeByteData)
	if v.desc.GetTimeCount() == conf.DeltaBlockSize {
		v.timeBuf.Write(timeByteData)
		v.timeBuf.Write(timeByteData)
		v.timeBuf.Write(timeByteData)
		v.desc.SetTimeCount(0)
	}
	// log.Info("s.timeBuf size: %d", s.timeBuf.Len())
	// write value to byteBuffer
	v.valueBuf.Write(valueByteData)
	// log.Info("s.valueBuf size: %d", s.valueBuf.Len())
	return
}

func (v *ValueWriter) Reset() {
	v.timeBuf.Reset()
	v.valueBuf.Reset()
	return
}

func NewValueWriter(d *sensorDescriptor.SensorDescriptor) (*ValueWriter, error) {
	return &ValueWriter{
		//sensorId:sId,
		timeBuf:      bytes.NewBuffer([]byte{}),
		valueBuf:     bytes.NewBuffer([]byte{}),
		desc:         d,
		timeEncoder:  d.GetTimeEncoder(),
		valueEncoder: d.GetValueEncoder(),
	}, nil
}
