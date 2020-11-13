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
	"encoding/json"
)

var BoolV bool
var IntV int32
var LongV int64
var FloatV float32
var DoubleV float64
var BinaryV []byte

type Field struct {
	DataType int32
}

func Copy(field Field) Field {
	outField := NewField(field.DataType)
	/* if outField.DataType != (int32{}) {
		 switch outField.DataType {
		 case DOUBLE:
			 outField.SetDoubleV(field.GetDoubleV())
			 break
		 case INT32:
			 outField.SetIntV(field.GetIntV())
			 break
		 case BOOLEAN:
			 outField.SetBoolV(field.GetBoolV())
			 break
		 case INT64:
			 outField.setLongV(field.GetLongV())
			 break
		 case FLOAT:
			 outField.SetFloatV(field.GetFloatV())
			 break
		 case TEXT:
			 outField.SetBinaryV(field.GetBinaryV())
			 break
		 default:
			 println("not support")
		 }
	 }*/
	return outField
}

func NewField(DataType int32) Field {
	return Field{DataType}
}

func (field *Field) GetBoolV() bool {
	return BoolV
}

func (field *Field) SetBoolV(boolV bool) {
	BoolV = boolV
}

func (field *Field) GetBinaryV() []byte {
	return BinaryV
}

func (field *Field) SetBinaryV(binaryV []byte) {
	BinaryV = binaryV
}

func (field *Field) GetFloatV() float32 {
	return FloatV
}

func (field *Field) SetFloatV(floatV float32) {
	FloatV = floatV
}

func (field *Field) GetDoubleV() float64 {
	return DoubleV
}

func (field *Field) SetDoubleV(doubleV float64) {
	DoubleV = doubleV
}

func (field *Field) GetIntV() int32 {
	return IntV
}

func (field *Field) SetIntV(intV int32) {
	IntV = intV
}

func (field *Field) GetLongV() int64 {
	return LongV
}

func (field *Field) setLongV(longV int64) {
	LongV = longV
}

func (field *Field) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"DataType": field.DataType,
	})
}
