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

package impl

import (
	"errors"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/read/reader/impl/basic"
)

type RowRecordTimestampGenerator struct {
	reader reader.IRowRecordReader
	filter filter.Filter

	currTime  int64
	exhausted bool
}

func (gen *RowRecordTimestampGenerator) Close() {
	gen.reader.Close()
}

func NewRowRecordTimestampGenerator(paths []string, readerMap map[string]reader.TimeValuePairReader, filter filter.Filter) *RowRecordTimestampGenerator {
	reader := basic.NewRecordReader(paths, readerMap)
	return &RowRecordTimestampGenerator{reader: reader, filter: filter, currTime: constant.INVALID_TIMESTAMP, exhausted: false}
}

func (gen *RowRecordTimestampGenerator) fetch() {
	for gen.reader.HasNext() {
		record, err := gen.reader.Next()
		if err != nil {
			log.Error("cannot read next RowRecord", err)
			gen.exhausted = true
			gen.currTime = constant.INVALID_TIMESTAMP
		}
		if gen.filter == nil || gen.filter.Satisfy(record) {
			gen.currTime = record.Timestamp()
			break
		}
	}
}

func (gen *RowRecordTimestampGenerator) HasNext() bool {
	if gen.exhausted {
		return false
	}
	if gen.currTime != constant.INVALID_TIMESTAMP {
		return true
	}
	gen.fetch()
	return gen.currTime != constant.INVALID_TIMESTAMP
}

func (gen *RowRecordTimestampGenerator) Next() (int64, error) {
	if gen.exhausted {
		return constant.INVALID_TIMESTAMP, errors.New("timestamp exhausted")
	}
	if gen.currTime == constant.INVALID_TIMESTAMP {
		gen.fetch()
		if gen.currTime == constant.INVALID_TIMESTAMP {
			return constant.INVALID_TIMESTAMP, errors.New("timestamp exhausted")
		}
	}
	ret := gen.currTime
	gen.currTime = constant.INVALID_TIMESTAMP
	return ret, nil
}
