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
)

type Field struct {
	DataType string
}

var (
	boolV   bool
	intV    int32
	longV   int64
	floatV  float32
	doubleV float64
	binaryV []byte
)

func Copy(field Field) Field {
	outField := NewField(field.DataType)
	if outField.DataType != "" {
		dateType := outField.DataType
		switch dateType {
		case "BOOLEAN":
			outField.SetBoolV(field.GetBoolV())
			break
		case "INT32":
			outField.SetIntV(field.GetIntV())
			break
		case "INT64":
			outField.SetLongV(field.GetLongV())
			break
		case "FLOAT":
			outField.SetFloatV(field.GetFloatV())
			break
		case "DOUBLE":
			outField.SetDoubleV(field.GetDoubleV())
			break
		case "TEXT":
			outField.SetBinaryV(field.GetBinaryV())
			break
		default:
			log.Error("not support dataType")
		}
	}
	return outField
}

func NewField(DataType string) Field {
	return Field{DataType}
}

func (f *Field) GetBoolV() bool {
	return boolV
}

func (f *Field) SetBoolV(boolVal bool) {
	boolV = boolVal
}

func (f *Field) GetBinaryV() []byte {
	return binaryV
}

func (f *Field) SetBinaryV(binaryVal []byte) {
	binaryV = binaryVal
}

func (f *Field) GetFloatV() float32 {
	return floatV
}

func (f *Field) SetFloatV(floatVal float32) {
	floatV = floatVal
}

func (f *Field) GetDoubleV() float64 {
	return doubleV
}

func (f *Field) SetDoubleV(doubleVal float64) {
	doubleV = doubleVal
}

func (f *Field) GetIntV() int32 {
	return intV
}

func (f *Field) SetIntV(intVal int32) {
	intV = intVal
}

func (f *Field) GetLongV() int64 {
	return longV
}

func (f *Field) SetLongV(longVal int64) {
	longV = longVal
}

func (f *Field) ToString() string {
	return f.GetStringValue()
}

func (f *Field) GetStringValue() string {
	if f.DataType == "" {
		return ""
	}
	dateType := f.DataType
	buf := bytes.NewBuffer([]byte{})
	switch dateType {
	case "BOOLEAN":
		binary.Write(buf, binary.BigEndian, &boolV)
		break
	case "INT32":
		binary.Write(buf, binary.BigEndian, &intV)
		break
	case "INT64":
		binary.Write(buf, binary.BigEndian, &longV)
		break
	case "FLOAT":
		binary.Write(buf, binary.BigEndian, &floatV)
		break
	case "DOUBLE":
		binary.Write(buf, binary.BigEndian, &doubleV)
		break
	case "TEXT":
		buf.Write(binaryV)
		break
	default:
		log.Error("not support dataType")
	}
	return buf.String()
}

func (f *Field) GetField(value interface{}, dateType string) *Field {
	if value == nil {
		return nil
	}
	field := NewField(dateType)
	switch dateType {
	case "BOOLEAN":
		field.SetBoolV(value.(bool))
		break
	case "INT32":
		field.SetIntV(int32(value.(int)))
		break
	case "INT64":
		field.SetLongV(int64(value.(int)))
		break
	case "FLOAT":
		field.SetFloatV(float32(value.(float64)))
		break
	case "DOUBLE":
		field.SetDoubleV(value.(float64))
		break
	case "TEXT":
		field.SetBinaryV([]byte(value.(string)))
		break
	default:
		log.Error("not support dataType")
	}
	return &field
}

func (f *Field) GetObjectValue(dateType string) interface{} {
	if f.DataType == "" {
		return nil
	}
	switch dateType {
	case "BOOLEAN":
		return f.GetBoolV()
		break
	case "INT32":
		return f.GetIntV()
		break
	case "INT64":
		return f.GetLongV()
		break
	case "FLOAT":
		return f.GetFloatV()
		break
	case "DOUBLE":
		return f.GetDoubleV()
		break
	case "TEXT":
		return f.GetBinaryV()
		break
	default:
		log.Error("not support dataType")
		return nil
	}
	return nil
}

func (f *Field) IsNull() bool {
	return f.DataType == ""
}
