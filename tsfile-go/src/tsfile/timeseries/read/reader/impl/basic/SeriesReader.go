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
	"tsfile/common/constant"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type SeriesReader struct {
	// page PageIndex and PageLimit, indicating the current page and the number of pages
	PageIndex int
	PageLimit int
	// Offsets and Sizes of every page of this series in a file
	Offsets    []int64
	Sizes      []int
	FileReader *read.TsFileSequenceReader
	PageReader reader.TimeValuePairReader
	DType      constant.TSDataType
	Encoding   constant.TSEncoding
}

func (r *SeriesReader) Read(data []byte) {
	panic("implement me")
}

func (r *SeriesReader) Skip() {
	r.Next()
}

func (r *SeriesReader) HasNext() bool {
	if r.PageReader != nil {
		if r.PageReader.HasNext() {
			return true
		} else if r.PageIndex < r.PageLimit-1 {
			r.nextPageReader()
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

func (r *SeriesReader) Next() (*datatype.TimeValuePair, error) {
	if r.PageReader.HasNext() {
		ret, err := r.PageReader.Next()
		if err != nil {
			return nil, err
		}
		return ret, nil
	} else {
		err := r.nextPageReader()
		if err != nil {
			return nil, err
		}
		return r.Next()
	}
}

func (r *SeriesReader) Close() {
	r.PageReader.Close()
	r.PageReader = nil
	r.PageIndex = r.PageLimit
	r.FileReader = nil
}

func NewSeriesReader(offsets []int64, sizes []int, reader *read.TsFileSequenceReader, dType constant.TSDataType, encoding constant.TSEncoding) *SeriesReader {
	return &SeriesReader{-1, len(offsets), offsets, sizes, reader, nil, dType, encoding}
}

func (r *SeriesReader) hasNextPageReader() bool {
	return r.PageIndex < r.PageLimit
}

func (r *SeriesReader) nextPageReader() error {
	r.PageIndex++
	if r.PageIndex >= r.PageLimit {
		return errors.New("page exhausted")
	}
	r.PageReader = &PageDataReader{DataType: r.DType, ValueDecoder: decoder.CreateDecoder(r.Encoding, r.DType),
		TimeDecoder: decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)}
	r.PageReader.Read(r.FileReader.ReadRaw(r.Offsets[r.PageIndex], r.Sizes[r.PageIndex]))
	return nil
}
