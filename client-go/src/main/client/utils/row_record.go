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
	"strconv"
)

type RowRecord struct {
	Timestamp int64
	Fields    []Field
}

func (r *RowRecord) AddField(field Field) {
	r.Fields[len(r.Fields)] = field
}

func (r *RowRecord) String() string {
	res := strconv.Itoa(int(r.Timestamp))
	for i := 0; i < len(r.Fields); i++ {
		res += "\t"
		if r.Fields[i].DataType == "" {
			res += "null"
			continue
		}
		objValue := r.Fields[i].GetObjectValue(r.Fields[i].DataType)
		switch r.Fields[i].DataType {
		case "BOOLEAN":
			res += strconv.FormatBool(objValue.(bool))
			break
		case "INT32":
			res += strconv.FormatInt(int64(objValue.(int32)), 10)
			break
		case "INT64":
			res += strconv.FormatInt(objValue.(int64), 10)
			break
		case "FLOAT":

			res += strconv.FormatFloat(float64(objValue.(float32)), 'f', 1, 32)
			break
		case "DOUBLE":
			res += strconv.FormatFloat(objValue.(float64), 'f', -1, 64)
			break
		case "TEXT":
			res += objValue.(string)
			break
		}
	}
	return res
}

