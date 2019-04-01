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
 * @Package Name: tsFileWriter
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-9-13 上午11:24
 * @Description:
 */

import (
	"bytes"
	"os"
	"tsfile/common/conf"
	"tsfile/common/log"
	"tsfile/common/utils"
	"tsfile/file/header"
	"tsfile/file/metadata"
	"tsfile/file/metadata/statistics"
	"tsfile/timeseries/write/fileSchema"
	"tsfile/timeseries/write/sensorDescriptor"
)

type TsFileIoWriter struct {
	tsIoFile                *os.File
	memBuf                  *bytes.Buffer
	currentRowGroupMetaData *metadata.RowGroupMetaData
	currentChunkMetaData    *metadata.ChunkMetaData
	rowGroupMetaDataSli     []*metadata.RowGroupMetaData
	rowGroupHeader          *header.RowGroupHeader
	chunkHeader             *header.ChunkHeader
}

const (
	MAXVALUE = "max_value"
	MINVALUE = "min_value"
	FIRST    = "first"
	SUM      = "sum"
	LAST     = "last"
)

func (t *TsFileIoWriter) GetTsIoFile() *os.File {
	return t.tsIoFile
}

func (t *TsFileIoWriter) GetPos() int64 {
	currentPos, _ := t.tsIoFile.Seek(0, os.SEEK_CUR)
	return currentPos
}

func (t *TsFileIoWriter) EndChunk(size int64, totalValueCount int64) {
	// set currentChunkMetaData
	t.currentChunkMetaData.SetTotalByteSizeOfPagesOnDisk(size)
	t.currentChunkMetaData.SetNumOfPoints(totalValueCount)
	t.currentRowGroupMetaData.AddChunkMetaData(t.currentChunkMetaData)
	// log.Info("end Chunk: %v, totalvalue: %v", t.currentChunkMetaData, totalValueCount)
	t.currentChunkMetaData = nil

	return
}

func (t *TsFileIoWriter) EndRowGroup(memSize int64) {
	t.currentRowGroupMetaData.SetTotalByteSize(memSize)
	t.rowGroupMetaDataSli = append(t.rowGroupMetaDataSli, t.currentRowGroupMetaData)
	//log.Info("end row group: %v", t.currentRowGroupMetaData)
	//t.currentRowGroupMetaData = nil
}

func (t *TsFileIoWriter) EndFile(fs fileSchema.FileSchema) {
	timeSeriesMap := fs.GetTimeSeriesMetaDatas()
	// log.Info("get time series map: %v", timeSeriesMap)
	tsDeviceMetaDataMap := make(map[string]*metadata.DeviceMetaData)
	for _, v := range t.rowGroupMetaDataSli {
		if v == nil {
			continue
		}
		currentDevice := v.GetDeviceId()
		if _, contain := tsDeviceMetaDataMap[currentDevice]; !contain {
			tsDeviceMetaData, _ := metadata.NewTimeDeviceMetaData()
			tsDeviceMetaDataMap[currentDevice] = tsDeviceMetaData
		}
		tdmd := tsDeviceMetaDataMap[currentDevice]
		tdmd.AddRowGroupMetaData(v)
		tsDeviceMetaDataMap[currentDevice] = tdmd
		// tsDeviceMetaDataMap[currentDevice].AddRowGroupMetaData(*v)
	}

	for _, tsDeviceMetaData := range tsDeviceMetaDataMap {
		startTime := int64(0x7fffffffffffffff)
		endTime := int64(0x0000000000000000)
		for _, rowGroup := range tsDeviceMetaData.GetRowGroups() {
			for _, timeSeriesChunkMetaData := range rowGroup.GetChunkMetaDataSli() {
				startTime = min(startTime, timeSeriesChunkMetaData.GetStartTime())
				endTime = max(endTime, timeSeriesChunkMetaData.GetEndTime())
			}
		}
		tsDeviceMetaData.SetStartTime(startTime)
		tsDeviceMetaData.SetEndTime(endTime)
	}
	tsFileMetaData, _ := metadata.NewTsFileMetaData(tsDeviceMetaDataMap, timeSeriesMap, conf.CurrentVersion)
	//footerIndex := t.GetPos()
	//log.Info("start to flush meta, file pos: %d", footerIndex)
	size := tsFileMetaData.SerializeTo(t.memBuf)
	// log.Info("t.memBuf: %s", t.memBuf)
	//log.Info("finish flushing meta %v, file pos: %d", tsFileMetaData, t.GetPos())
	t.memBuf.Write(utils.Int32ToByte(int32(size), 0))
	t.memBuf.Write([]byte(conf.MAGIC_STRING))

	// flush mem-filemeta to file
	t.WriteBytesToFile(t.memBuf)
	log.Info("file pos: %d", t.GetPos())
}

func max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func (t *TsFileIoWriter) WriteMagic() int {
	n, err := t.tsIoFile.Write([]byte(conf.MAGIC_STRING))
	if err != nil {
		log.Error("write start magic to file err: ", err)
	}
	return n
}

func (t *TsFileIoWriter) StartFlushRowGroup(deviceId string, rowGroupSize int64, seriesNumber int32) int {
	// timeSeriesChunkMetaDataMap := make(map[string]metaData.TimeSeriesChunkMetaData)
	timeSeriesChunkMetaDataSli := make([]*metadata.ChunkMetaData, 0)
	t.currentRowGroupMetaData, _ = metadata.NewRowGroupMetaData(deviceId, 0, t.GetPos(), timeSeriesChunkMetaDataSli)
	t.currentRowGroupMetaData.RecalculateSerializedSize()
	rowGroupHeader, _ := header.NewRowGroupHeader(deviceId, rowGroupSize, seriesNumber)
	//log.Info("rowGroupHeader: %v", rowGroupHeader)
	rowGroupHeader.RowGroupHeaderToMemory(t.memBuf)
	t.rowGroupHeader = rowGroupHeader
	// rowgroup header bytebuffer write to file
	t.WriteBytesToFile(t.memBuf)
	// truncate bytebuffer to empty
	t.memBuf.Reset()

	return header.GetRowGroupSerializedSize(deviceId)
}

func (t *TsFileIoWriter) StartFlushChunk(sd *sensorDescriptor.SensorDescriptor, compressionType int16,
	tsDataType int16, encodingType int16, statistics statistics.Statistics,
	maxTimestamp int64, minTimestamp int64, pageBufSize int, numOfPages int) int {
	t.currentChunkMetaData, _ = metadata.NewTimeSeriesChunkMetaData(sd.GetSensorId(), t.GetPos(), minTimestamp, maxTimestamp)
	chunkHeader, _ := header.NewChunkHeader(sd.GetSensorId(), pageBufSize, tsDataType, compressionType, encodingType, numOfPages, 0)
	chunkHeader.ChunkHeaderToMemory(t.memBuf)
	t.chunkHeader = chunkHeader
	// chunk header bytebuffer write to file
	t.WriteBytesToFile(t.memBuf)
	// truncate bytebuffer to empty
	t.memBuf.Reset()
	// set tsdigest
	tsDigest, _ := metadata.NewTsDigest()
	statisticsMap := make(map[string]*bytes.Buffer)
	//var max bytes.Buffer
	//max.Write(statistics.GetMaxByte(tsDataType))
	//statisticsMap[MAXVALUE] = max
	//var min bytes.Buffer
	//min.Write(statistics.GetMinByte(tsDataType))
	//statisticsMap[MINVALUE] = min
	//var first bytes.Buffer
	//first.Write(statistics.GetFirstByte(tsDataType))
	//statisticsMap[FIRST] = first
	//var sum bytes.Buffer
	//sum.Write(statistics.GetSumByte(tsDataType))
	//statisticsMap[SUM] = sum
	//var last bytes.Buffer
	//last.Write(statistics.GetLastByte(tsDataType))
	//statisticsMap[LAST] = last

	var min bytes.Buffer
	min.Write(statistics.GetMinByte(tsDataType))
	statisticsMap[MINVALUE] = &min
	var last bytes.Buffer
	last.Write(statistics.GetLastByte(tsDataType))
	statisticsMap[LAST] = &last
	var sum bytes.Buffer
	sum.Write(statistics.GetSumByte(tsDataType))
	statisticsMap[SUM] = &sum
	var first bytes.Buffer
	first.Write(statistics.GetFirstByte(tsDataType))
	statisticsMap[FIRST] = &first
	var max bytes.Buffer
	max.Write(statistics.GetMaxByte(tsDataType))
	statisticsMap[MAXVALUE] = &max

	tsDigest.SetStatistics(statisticsMap)
	t.currentChunkMetaData.SetDigest(tsDigest)
	return header.GetChunkSerializedSize(sd.GetSensorId())
}

func (t *TsFileIoWriter) WriteBytesToFile(buf *bytes.Buffer) {
	//声明一个空的slice,容量为timebuf的长度
	timeSlice := make([]byte, buf.Len())
	//把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
	buf.Read(timeSlice)
	t.tsIoFile.Write(timeSlice)
	return
}

func NewTsFileIoWriter(file string) (*TsFileIoWriter, error) {
	newFile, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Info("open file:%s failed.", file)
	}

	return &TsFileIoWriter{
		tsIoFile:            newFile,
		memBuf:              bytes.NewBuffer([]byte{}),
		rowGroupMetaDataSli: make([]*metadata.RowGroupMetaData, 0),
	}, nil
}
