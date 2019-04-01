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

package utils

import (
	"bytes"
	"encoding/binary"
	"math"
	"tsfile/common/log"
)

/**
 * @Package Name: utils
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-9-3 下午5:04
 * @Description:
 */

// bool
func BoolToByte(flag bool, endianType int16) []byte {
	var buffer bytes.Buffer
	var err error
	if endianType == 0 { // BigEdian
		err = binary.Write(&buffer, binary.BigEndian, flag)
	} else { // LittleEdian
		err = binary.Write(&buffer, binary.LittleEndian, flag)
	}

	if err != nil {
		log.Error("BoolToByte error : %s", err)
		return nil
	}
	return buffer.Bytes()
}

// int
func Int64ToByte(num int64, endianType int16) []byte {
	var buffer bytes.Buffer
	var err error
	if endianType == 0 {
		err = binary.Write(&buffer, binary.BigEndian, num)
	} else {
		err = binary.Write(&buffer, binary.LittleEndian, num)
	}

	if err != nil {
		log.Error("Int64ToByte error : %s", err)
		return nil
	}
	return buffer.Bytes()
}

//func Int64ToByteLittleEndian(num int64) []byte {
//	var buffer bytes.Buffer
//	err := binary.Write(&buffer, binary.LittleEndian, num)
//	if err != nil {
//		log.Error("Int64ToByte error : %s", err)
//		return nil
//	}
//	return buffer.Bytes()
//}

func Int32ToByte(num int32, endianType int16) []byte {
	var buffer bytes.Buffer
	var err error
	if endianType == 0 {
		err = binary.Write(&buffer, binary.BigEndian, num)
	} else {
		err = binary.Write(&buffer, binary.LittleEndian, num)
	}

	if err != nil {
		log.Error("Int32ToByte error : %s", err)
		return nil
	}
	return buffer.Bytes()
}

//func Int32ToByteLittleEndian(num int32) []byte {
//	var buffer bytes.Buffer
//	err := binary.Write(&buffer, binary.LittleEndian, num)
//	if err != nil {
//		log.Error("Int32ToByte error : %s", err)
//		return nil
//	}
//	return buffer.Bytes()
//}

func Int16ToByte(num int16, endianType int16) []byte {
	var buffer bytes.Buffer
	var err error
	if endianType == 0 {
		err = binary.Write(&buffer, binary.BigEndian, num)
	} else {
		err = binary.Write(&buffer, binary.LittleEndian, num)
	}

	if err != nil {
		log.Error("Int16ToByte error : %s", err)
		return nil
	}
	return buffer.Bytes()
}

// float
func Float32ToByte(float float32, endianType int16) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	if endianType == 0 {
		binary.BigEndian.PutUint32(bytes, bits)
	} else {
		binary.LittleEndian.PutUint32(bytes, bits)
	}

	return bytes
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)

	return math.Float32frombits(bits)
}

func Float64ToByte(float float64, endianType int16) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	if endianType == 0 {
		binary.BigEndian.PutUint64(bytes, bits)
	} else {
		binary.LittleEndian.PutUint64(bytes, bits)
	}

	return bytes
}

func ByteToFloat64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)

	return math.Float64frombits(bits)
}
