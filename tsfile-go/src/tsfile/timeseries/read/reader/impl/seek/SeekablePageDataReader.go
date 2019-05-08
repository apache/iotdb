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

package seek

import (
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader/impl/basic"
)

type SeekablePageDataReader struct {
	*basic.PageDataReader

	current *datatype.TimeValuePair
}

func (r *SeekablePageDataReader) Next() (*datatype.TimeValuePair, error) {
	row, err := r.PageDataReader.Next()
	r.current = row
	return r.current, err
}

func (r *SeekablePageDataReader) Current() *datatype.TimeValuePair {
	return r.current
}

func (r *SeekablePageDataReader) Seek(timestamp int64) bool {
	if r.current == nil {
		if r.HasNext() {
			r.Next()
		} else {
			return false
		}
	}
	for {
		if r.current.Timestamp < timestamp {
			if r.HasNext() {
				r.Next()
				continue
			} else {
				return false
			}
		} else if r.current.Timestamp == timestamp {
			return true
		} else {
			return false
		}
	}
}
