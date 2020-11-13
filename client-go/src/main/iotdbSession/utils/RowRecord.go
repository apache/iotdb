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
	"github.com/typa01/go-utils"
)

type RowRecord struct {
	Timestamp int64
	Fields    []Field
}

func (rowRecord *RowRecord) AddField(field Field) {
	rowRecord.Fields[len(rowRecord.Fields)] = field
}

func (rowRecord *RowRecord) ToString() string {
	stringBuilder := tsgutils.NewStringBuilder()
	stringBuilder.AppendInt64(rowRecord.Timestamp)
	for i := 0; i < len(rowRecord.Fields); i++ {
		stringBuilder.Append("\t")
		field, _ := rowRecord.Fields[i].MarshalJSON()
		stringBuilder.Append(string(field))
	}
	return stringBuilder.ToString()
}

func (rowRecord *RowRecord) SetTimestamp(timestamp int64) {
	rowRecord.Timestamp = timestamp
}

func (rowRecord *RowRecord) GetTimestamp() int64 {
	return rowRecord.Timestamp
}

func (rowRecord *RowRecord) GetFields() []Field {
	return rowRecord.Fields
}

func (rowRecord *RowRecord) SetFields(fields []Field) {
	rowRecord.Fields = fields
}

func (rowRecord *RowRecord) SetField(index int, field Field) {
	rowRecord.Fields[index] = field
}
