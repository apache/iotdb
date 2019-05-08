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
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/encoding/decoder"
	"tsfile/file/header"
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader/impl/basic"
)

type SeekableSeriesReader struct {
	*basic.SeriesReader

	pageHeaders []*header.PageHeader
	current     *datatype.TimeValuePair
	exhausted   bool
}

func (r *SeekableSeriesReader) Seek(timestamp int64) bool {

	// seek the page that may contain the given timestamp
	pageChanged := false
	if r.PageIndex == -1 {
		r.PageIndex = 0
		pageChanged = true
	}
	for r.PageIndex < r.PageLimit &&
		!(r.pageHeaders[r.PageIndex].Min_timestamp() <= timestamp && timestamp <= r.pageHeaders[r.PageIndex].Max_timestamp()) {
		r.PageIndex++
		pageChanged = true
	}
	if pageChanged {
		if r.PageIndex < r.PageLimit {
			r.PageIndex--
			r.nextPageReader()
		} else {
			return false
		}
	}

	// seek within this page
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

func (r *SeekableSeriesReader) Current() *datatype.TimeValuePair {
	return r.current
}

func NewSeekableSeriesReader(offsets []int64, sizes []int, reader *read.TsFileSequenceReader, pageHeaders []*header.PageHeader, dType constant.TSDataType, encoding constant.TSEncoding) *SeekableSeriesReader {
	return &SeekableSeriesReader{&basic.SeriesReader{-1, len(offsets),
		offsets, sizes, reader, nil, dType, encoding}, pageHeaders, nil, false}
}

func (r *SeekableSeriesReader) hasNextPageReader() bool {
	return r.PageIndex < r.PageLimit
}

func (r *SeekableSeriesReader) nextPageReader() error {
	r.PageIndex++
	if r.PageIndex >= r.PageLimit {
		return errors.New("page exhausted")
	}
	r.PageReader = &SeekablePageDataReader{&basic.PageDataReader{DataType: r.DType, ValueDecoder: decoder.CreateDecoder(r.Encoding, r.DType),
		TimeDecoder: decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)}, nil}
	r.PageReader.Read(r.FileReader.ReadRaw(r.Offsets[r.PageIndex], r.Sizes[r.PageIndex]))
	return nil
}

func (r *SeekableSeriesReader) HasNext() bool {
	if r.exhausted {
		return false
	}
	if r.PageReader != nil {
		if r.PageReader.HasNext() {
			return true
		} else if r.PageIndex < r.PageLimit-1 {
			if err := r.nextPageReader(); err != nil {
				log.Error("cannot read next page", err)
				r.exhausted = true
				return false
			}
			return r.HasNext()
		} else {
			return false
		}
	} else if r.PageIndex < r.PageLimit-1 {
		r.nextPageReader()
		return r.HasNext()
	}
	return false
}

func (r *SeekableSeriesReader) Next() (*datatype.TimeValuePair, error) {
	if r.exhausted {
		return nil, errors.New("series exhausted")
	}
	if r.PageReader.HasNext() {
		tv, err := r.PageReader.Next()
		if err != nil {
			return nil, err
		}
		r.current = tv
		return r.current, nil
	} else {
		r.nextPageReader()
		return r.Next()
	}
}
