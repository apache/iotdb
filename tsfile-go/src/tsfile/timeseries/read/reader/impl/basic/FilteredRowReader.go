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

package basic

import (
	"errors"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type FilteredRowReader struct {
	reader *RowRecordReader
	filter filter.Filter

	row       *datatype.RowRecord
	exhausted bool
}

func (r *FilteredRowReader) fillCache() {
	for {
		if !r.reader.HasNext() {
			return
		} else {
			row, err := r.reader.Next()
			if err != nil {
				r.row = nil
				return
			}
			if r.filter == nil || r.filter.Satisfy(row) {
				r.row = row
				//fmt.Printf("Row %v satisfies\n", row.Timestamp())
				break
			}
		}
	}
}

func (r *FilteredRowReader) HasNext() bool {
	if r.exhausted {
		return false
	}
	if r.row == nil {
		r.fillCache()
		if r.row == nil {
			r.exhausted = true
			return false
		}
	}
	return r.row != nil
}

func (r *FilteredRowReader) Next() (*datatype.RowRecord, error) {
	if r.row == nil {
		r.fillCache()
		if r.row == nil {
			r.exhausted = true
			return nil, errors.New("RowReader exhausted")
		}
	}
	ret := r.row
	r.row = nil

	return ret, nil
}

func (r *FilteredRowReader) Close() {
	r.reader.Close()
}

func NewFilteredRowReader(paths []string, readerMap map[string]reader.TimeValuePairReader, filter filter.Filter) *FilteredRowReader {
	rowReader := NewRecordReader(paths, readerMap)
	dataSet := &FilteredRowReader{reader: rowReader, filter: filter, exhausted: false}
	return dataSet
}
