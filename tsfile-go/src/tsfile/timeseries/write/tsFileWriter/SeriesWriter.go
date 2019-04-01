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

package tsFileWriter

/**
 * @Package Name: seriesWriter
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-28 下午8:28
 * @Description:
 */

import (
	"tsfile/common/conf"
	"tsfile/file/header"
	"tsfile/file/metadata/statistics"
	"tsfile/timeseries/write/sensorDescriptor"
)

type SeriesWriter struct {
	deviceId          string
	dataSeriesWriters map[string]SeriesWriter

	desc       *sensorDescriptor.SensorDescriptor
	tsDataType int16
	pageWriter *PageWriter
	/* page size threshold 	*/
	psThres             int
	pageCountUpperBound int
	/* value writer to encode data*/
	valueWriter ValueWriter
	/* value count on a page. It will be reset agter calling */
	valueCount                 int
	valueCountForNextSizeCheck int
	/*statistics on a page. It will be reset after calling */
	pageStatistics             statistics.Statistics
	seriesStatistics           statistics.Statistics
	time                       int64
	minTimestamp               int64
	sensorDescriptor           sensorDescriptor.SensorDescriptor
	minimumRecordCountForCheck int
	numOfPages                 int
}

func (s *SeriesWriter) GetTsDataType() int16 {
	return s.tsDataType
}

func (s *SeriesWriter) GetTsDeviceId() string {
	return s.deviceId
}

func (s *SeriesWriter) GetNumOfPages() int {
	return s.numOfPages
}

func (s *SeriesWriter) GetCurrentChunkSize(sId string) int {
	//return int64(tfiw.chunkHeader.GetChunkSerializedSize()) + s.pageWriter.GetCurrentDataSize()
	chunkHeaderSize := header.GetChunkSerializedSize(sId)
	size := chunkHeaderSize + s.pageWriter.GetCurrentDataSize()
	return size
}

func (s *SeriesWriter) Write(t int64, data *DataPoint) bool {
	s.time = t
	//s.valueCount = s.valueCount + 1

	vw := &(s.valueWriter)
	vw.timeEncoder.Encode(t, vw.timeBuf)
	switch s.tsDataType {
	case 0, 1, 2, 3, 4, 5:
		vw.valueEncoder.Encode(data.value, vw.valueBuf)
	default:
	}
	//s.valueWriter.Write(t, s.tsDataType, data, s.valueCount)
	//logcost.CostWriteTimesTest5 += int64(time.Since(tsCurNew))
	s.valueCount = s.valueCount + 1
	// statistics ignore here, if necessary, Statistics.java
	s.pageStatistics.UpdateStats(data.value)

	if s.minTimestamp == -1 {
		s.minTimestamp = t
	}
	// check page size and write page data to buffer
	s.checkPageSizeAndMayOpenNewpage()
	return true
}

func (s *SeriesWriter) WriteToFileWriter(tsFileIoWriter *TsFileIoWriter) {
	// write all pages in the same chunk to file
	s.pageWriter.WriteAllPagesOfSeriesToTsFile(tsFileIoWriter, s.seriesStatistics, s.numOfPages)
	// reset pageWriter
	s.pageWriter.Reset()
	// reset series_statistics
	s.seriesStatistics = statistics.GetStatsByType(s.tsDataType)
}

func (s *SeriesWriter) checkPageSizeAndMayOpenNewpage() {
	if s.valueCount == conf.MaxNumberOfPointsInPage {
		//log.Info("current line count reaches the upper bound, write page %s", s.sensorDescriptor)
		// write data to buffer
		s.WritePage()
	} else if s.valueCount >= s.valueCountForNextSizeCheck {
		currentColumnSize := s.valueWriter.GetCurrentMemSize()
		if currentColumnSize > s.psThres {
			// write data to buffer
			s.WritePage()
		} //else {
		//	log.Info("not enough size to write disk now.")
		//}
		// int * 1.0 / int 为float， 再乘以valueCount，得到下次检查的count
		s.valueCountForNextSizeCheck = s.psThres * 1.0 / currentColumnSize * s.valueCount
	}
}

func (s *SeriesWriter) PreFlush() {
	if s.valueCount > 0 {
		s.WritePage()
	}
}

func (s *SeriesWriter) EstimateMaxSeriesMemSize() int64 {
	valueMemSize := s.valueWriter.timeBuf.Len() + s.valueWriter.valueBuf.Len()
	pageMemSize := s.pageWriter.EstimateMaxPageMemSize()
	return int64(valueMemSize + pageMemSize)
}

func (s *SeriesWriter) WritePage() {
	s.pageWriter.WritePageHeaderAndDataIntoBuff(s.valueWriter.GetByteBuffer(),
		s.valueCount, s.pageStatistics, s.time, s.minTimestamp)
	// pageStatistics
	s.numOfPages += 1

	s.minTimestamp = -1
	s.valueCount = 0
	s.valueWriter.Reset()
	s.ResetPageStatistics()
	return
}

func (s *SeriesWriter) ResetPageStatistics() {
	s.pageStatistics = statistics.GetStatsByType(s.tsDataType)
	return
}

func NewSeriesWriter(dId string, d *sensorDescriptor.SensorDescriptor, pw *PageWriter, pst int) (*SeriesWriter, error) {
	vw, _ := NewValueWriter(d)
	return &SeriesWriter{
		deviceId:                   dId,
		desc:                       d,
		pageWriter:                 pw,
		psThres:                    pst,
		pageCountUpperBound:        conf.MaxNumberOfPointsInPage,
		minimumRecordCountForCheck: 1,
		valueCountForNextSizeCheck: 1,
		numOfPages:                 0,
		tsDataType:                 d.GetTsDataType(),
		seriesStatistics:           statistics.GetStatsByType(d.GetTsDataType()),
		pageStatistics:             statistics.GetStatsByType(d.GetTsDataType()),
		valueWriter:                *vw,
		minTimestamp:               -1,
		valueCount:                 0,
	}, nil
}
