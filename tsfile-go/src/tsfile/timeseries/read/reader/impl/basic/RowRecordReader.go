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
	"math"
	"tsfile/common/log"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type RowRecordReader struct {
	paths     []string
	readerMap map[string]reader.TimeValuePairReader

	cacheList []*datatype.TimeValuePair
	row       *datatype.RowRecord
	currTime  int64
	exhausted bool
}

func NewRecordReader(paths []string, readerMap map[string]reader.TimeValuePairReader) *RowRecordReader {
	ret := &RowRecordReader{paths: paths, readerMap: readerMap}
	ret.row = datatype.NewRowRecordWithPaths(paths)
	ret.cacheList = make([]*datatype.TimeValuePair, len(paths))
	ret.currTime = math.MaxInt64
	ret.exhausted = false

	return ret
}

func (r *RowRecordReader) fillCache() error {
	// try filling the column caches and update the currTime
	for i, path := range r.paths {
		if r.cacheList[i] == nil && r.readerMap[path].HasNext() {
			tv, err := r.readerMap[path].Next()
			if err != nil {
				r.exhausted = true
				return err
			}
			r.cacheList[i] = tv
		}
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp < r.currTime {
			r.currTime = r.cacheList[i].Timestamp
		}
	}
	return nil
}

func (r *RowRecordReader) fillRow() {
	// fill the row cache using column caches
	r.row = datatype.NewRowRecordWithPaths(r.paths)
	for i, _ := range r.paths {
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp == r.currTime {
			r.row.Values()[i] = r.cacheList[i].Value
			r.cacheList[i] = nil
		} else {
			r.row.Values()[i] = nil
		}
	}
	r.row.SetTimestamp(r.currTime)
}

func (r *RowRecordReader) HasNext() bool {
	if r.currTime != math.MaxInt64 {
		return true
	}
	err := r.fillCache()
	if err != nil {
		log.Error("Cannot read next record ", err)
		r.exhausted = true
	} else if r.currTime == math.MaxInt64 {
		r.exhausted = true
	}
	return !r.exhausted
}

func (r *RowRecordReader) Next() (*datatype.RowRecord, error) {
	if r.exhausted {
		return nil, errors.New("RowRecord exhausted")
	}
	if r.currTime == math.MaxInt64 {
		err := r.fillCache()
		if err != nil {
			r.exhausted = true
			return nil, err
		}
		if r.currTime == math.MaxInt64 {
			r.exhausted = true
			return nil, errors.New("RowRecord exhausted")
		}
	}
	r.fillRow()
	r.currTime = math.MaxInt64

	return r.row, nil
}

func (r *RowRecordReader) Close() {
	for _, path := range r.paths {
		if r.readerMap[path] != nil {
			r.readerMap[path].Close()
		}
	}
}
