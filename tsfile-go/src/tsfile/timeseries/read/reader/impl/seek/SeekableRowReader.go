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
	"errors"
	"math"
	"tsfile/common/log"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type SeekableRowReader struct {
	paths     []string
	readerMap map[string]reader.ISeekableTimeValuePairReader

	cacheList []*datatype.TimeValuePair
	current   *datatype.RowRecord
	currTime  int64
	exhausted bool
}

func (r *SeekableRowReader) Current() *datatype.RowRecord {
	return r.current
}

func (r *SeekableRowReader) Seek(timestamp int64) bool {
	hasRecord := false
	r.currTime = timestamp
	for i, path := range r.paths {
		if r.readerMap[path].Seek(timestamp) {
			tv := r.readerMap[path].Current()
			r.cacheList[i] = tv
			hasRecord = true
		} else {
			r.cacheList[i] = nil
		}
	}
	r.fillRow()
	return hasRecord
}

func NewSeekableRowReader(paths []string, readerMap map[string]reader.ISeekableTimeValuePairReader) *SeekableRowReader {
	ret := &SeekableRowReader{paths, readerMap, make([]*datatype.TimeValuePair, len(paths)),
		datatype.NewRowRecordWithPaths(paths), math.MaxInt64, false}
	return ret
}

func (r *SeekableRowReader) fillCache() error {
	// try filling the column caches and update the currTime
	for i, path := range r.paths {
		if r.cacheList[i] == nil && r.readerMap[path].HasNext() {
			tv, err := r.readerMap[path].Next()
			if err != nil {
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

func (r *SeekableRowReader) fillRow() {
	// fill the current cache using column caches
	r.current = datatype.NewRowRecordWithPaths(r.paths)
	for i, _ := range r.paths {
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp == r.currTime {
			r.current.Values()[i] = r.cacheList[i].Value
			r.cacheList[i] = nil
		} else {
			r.current.Values()[i] = nil
		}
	}
	r.current.SetTimestamp(r.currTime)
}

func (r *SeekableRowReader) HasNext() bool {
	if r.currTime != math.MaxInt64 {
		return true
	}
	err := r.fillCache()
	if err != nil {
		log.Error("RowRecord exhausted")
		r.exhausted = true
	} else if r.current.Timestamp() == math.MaxInt64 {
		r.exhausted = true
	}
	return !r.exhausted
}

func (r *SeekableRowReader) Next() (*datatype.RowRecord, error) {
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

	return r.current, nil
}

func (r *SeekableRowReader) Close() {
	for _, path := range r.paths {
		if r.readerMap[path] != nil {
			r.readerMap[path].Close()
		}
	}
}
