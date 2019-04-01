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
 * @Package Name: rowGroupWriter
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-28 下午6:19
 * @Description:
 */

import (
	"tsfile/common/log"
	_ "tsfile/common/utils"
	"tsfile/file/header"
	"tsfile/timeseries/write/sensorDescriptor"
)

type RowGroupWriter struct {
	deviceId          string
	dataSeriesWriters map[string]*SeriesWriter
}

func (r *RowGroupWriter) AddSeriesWriter(sd *sensorDescriptor.SensorDescriptor, pageSize int) {
	//start_edit wangcan 2018-10-15
	//if contain, _ := utils.MapContains(r.dataSeriesWriters, sd.GetSensorId()); !contain {
	_, contain := r.dataSeriesWriters[sd.GetSensorId()]
	if !contain {
		//end_edit
		// new pagewriter
		pw, _ := NewPageWriter(sd)

		// new serieswrite
		r.dataSeriesWriters[sd.GetSensorId()], _ = NewSeriesWriter(r.deviceId, sd, pw, pageSize)
		//sw, _ := NewSeriesWriter(r.deviceId, sd, pw, pageSize)
		//r.dataSeriesWriters[sd.GetSensorId()] = sw
		//start_edit wangcan 2018-10-15
		//if input same sessor id and log again, comment it
		//} else {
		//	log.Info("given sensor has exist, need not add to series writer again.")
		//end_edit
	}
	return
}

func (r *RowGroupWriter) Write(t int64, data []*DataPoint) {
	for _, v := range data {
		//start_edit wangcan 2018-10-15
		//if ok, _ := utils.MapContains(r.dataSeriesWriters, v.GetSensorId()); ok {
		//	v.Write(t, r.dataSeriesWriters[v.GetSensorId()])
		dataSW, ok := r.dataSeriesWriters[v.GetSensorId()]
		if ok {
			if dataSW.GetTsDeviceId() == "" {
				log.Info("give seriesWriter is null, do nothing and return.")
			} else {
				dataSW.Write(t, v)
			}
			//v.Write(t, dataSeriesWriter)
		} else {
			log.Error("time: %d, sensor id %s not found! ", t, v.GetSensorId())
		}
	}
	return
}

func (r *RowGroupWriter) FlushToFileWriter(tsFileIoWriter *TsFileIoWriter) {
	for _, v := range r.dataSeriesWriters {
		v.WriteToFileWriter(tsFileIoWriter)
	}
	return
}

func (r *RowGroupWriter) PreFlush() {
	// flush current pages to mem.
	for _, v := range r.dataSeriesWriters {
		v.PreFlush()
	}
	return
}

func (r *RowGroupWriter) GetCurrentRowGroupSize() int {
	// get current size
	//size := int64(tfiw.rowGroupHeader.GetRowGroupSerializedSize())
	rowGroupHeaderSize := header.GetRowGroupSerializedSize(r.deviceId)
	size := rowGroupHeaderSize
	for k, v := range r.dataSeriesWriters {
		size += v.GetCurrentChunkSize(k)
	}

	return size
}

func (r *RowGroupWriter) GetSeriesNumber() int32 {
	return int32(len(r.dataSeriesWriters))
}

func (r *RowGroupWriter) UpdateMaxGroupMemSize() int64 {
	var bufferSize int64
	for _, v := range r.dataSeriesWriters {
		bufferSize += v.EstimateMaxSeriesMemSize()
	}
	return bufferSize
}

func (r *RowGroupWriter) Close() bool {
	return true
}

func NewRowGroupWriter(dId string) (*RowGroupWriter, error) {
	return &RowGroupWriter{
		deviceId:          dId,
		dataSeriesWriters: make(map[string]*SeriesWriter),
	}, nil
}
