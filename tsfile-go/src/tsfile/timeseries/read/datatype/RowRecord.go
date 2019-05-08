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

package datatype

type RowRecord struct {
	timestamp int64
	paths     []string
	values    []interface{}
}

func (r *RowRecord) SetTimestamp(timestamp int64) {
	r.timestamp = timestamp
}

func NewRowRecord() *RowRecord {
	return &RowRecord{0, nil, nil}
}

func NewRowRecordWithPaths(paths []string) *RowRecord {
	return &RowRecord{0, paths, make([]interface{}, len(paths))}
}

func (r *RowRecord) Values() []interface{} {
	return r.values
}

func (r *RowRecord) Paths() []string {
	return r.paths
}

func (r *RowRecord) Timestamp() int64 {
	return r.timestamp
}
