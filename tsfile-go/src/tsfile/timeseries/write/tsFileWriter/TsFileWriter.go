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
 * @Package Name: write
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-24 下午5:41
 * @Description:
 */

import (
	_ "time"
	"tsfile/common/conf"
	"tsfile/common/log"
	"tsfile/timeseries/write/fileSchema"
	"tsfile/timeseries/write/sensorDescriptor"
)

type TsFileWriter struct {
	tsFileIoWriter             *TsFileIoWriter
	schema                     *fileSchema.FileSchema
	recordCount                int64
	recordCountForNextMemCheck int64
	rowGroupSizeThreshold      int64
	primaryRowGroupSize        int64
	pageSize                   int64
	oneRowMaxSize              int
	groupDevices               map[string]*RowGroupWriter
	lastGroupDevice            *RowGroupWriter
	lastSeriesWriter           *SeriesWriter
	lastSessorId               string
}

func (t *TsFileWriter) AddSensor(sd *sensorDescriptor.SensorDescriptor) []byte {
	if _, ok := t.schema.GetSensorDescriptiorMap()[sd.GetSensorId()]; !ok {
		t.schema.GetSensorDescriptiorMap()[sd.GetSensorId()] = sd
	} else {
		log.Info("the given sensor has exist!")
	}
	t.schema.Registermeasurement(sd)
	t.oneRowMaxSize = t.schema.GetCurrentRowMaxSize()
	//if t.primaryRowGroupSize <= int64(t.oneRowMaxSize) {
	//	log.Info("AddSensor error: the potential size of one row is too large.")
	//}
	t.rowGroupSizeThreshold = t.primaryRowGroupSize - int64(t.oneRowMaxSize)

	// flush rowgroup
	t.checkMemorySizeAndMayFlushGroup()
	return nil
}

//func (t *TsFileWriter)checkMemorySize()(bool){
//	if t.recordCount >= t.recordCountForNextMemCheck {
//		// calculate all group size
//		memSize := t.CalculateMemSizeForAllGroup()
//		if memSize > t.rowGroupSizeThreshold {
//			log.Info("start_write_row_group, memory space occupy: %s", memSize)
//			t.recordCountForNextMemCheck = t.rowGroupSizeThreshold / int64(t.oneRowMaxSize)
//			return t.flushAllRowGroups(false)
//		} else {
//			t.recordCountForNextMemCheck = t.recordCount + (t.rowGroupSizeThreshold - memSize) / int64(t.oneRowMaxSize)
//			return false
//		}
//	}
//	return false
//}

/**
 * flush the data in all series writers and their page writers to outputStream.
 * @param isFillRowGroup whether to fill RowGroup
 * @return true - size of tsfile or metadata reaches the threshold.
 * 		 false - otherwise. But this function just return false, the Override of IoTDB may return true.
 */
func (t *TsFileWriter) flushAllRowGroups(isFillRowGroup bool) bool {
	// flush data to disk
	if t.recordCount > 0 {
		totalMemStart := t.tsFileIoWriter.GetPos()
		if t.recordCount > 0 {
			for _, v := range t.groupDevices {
				v.PreFlush()
			}
			for k, v := range t.groupDevices {
				groupDevice := t.groupDevices[k]
				//rowGroupSize := 1 * 4 + 1 * 8 + len(v.deviceId) + 1 * 4
				rowGroupSize := v.GetCurrentRowGroupSize()
				// write rowgroup header to file
				t.tsFileIoWriter.StartFlushRowGroup(k, int64(rowGroupSize), groupDevice.GetSeriesNumber())
				// write chunk to file
				groupDevice.FlushToFileWriter(t.tsFileIoWriter)
				// finished write file(and then write filemeta to file)
				t.tsFileIoWriter.EndRowGroup(t.tsFileIoWriter.GetPos() - totalMemStart)
			}
		}
		//log.Info("write to rowGroup end!")
		t.recordCount = 0
		t.reset()
	}
	return true
}

func (t *TsFileWriter) reset() {
	for k, _ := range t.groupDevices {
		delete(t.groupDevices, k)
	}
}

func (t *TsFileWriter) Write(tr *TsRecord) bool {
	// write data here
	//gd, ok := t.checkIsDeviceExist(tr, t.schema)
	//tsCurNew2 := time.Now()
	var ok bool
	var gd *RowGroupWriter
	var valueWriter *ValueWriter
	var sessorID string
	var dataSW *SeriesWriter
	//var dataSWLast *SeriesWriter

	// check device
	var strDeviceID string = tr.GetDeviceId()
	if t.lastGroupDevice != nil && (t.lastGroupDevice.deviceId == strDeviceID) {
		gd = t.lastGroupDevice
		ok = true
	} else {
		gd, ok = t.groupDevices[strDeviceID]
		if !ok {
			// if not exist
			gd, _ = NewRowGroupWriter(strDeviceID)
			t.groupDevices[strDeviceID] = gd
		}
		t.lastGroupDevice = gd
		t.lastSeriesWriter = nil
		t.lastSessorId = ""
	}

	timeST := tr.GetTime()
	schemaSensorDescriptorMap := t.schema.GetSensorDescriptiorMap()
	data := tr.GetDataPointSli()
	//log.CostWriteTimesTest2 += int64(time.Since(tsCurNew2))
	for _, v := range data {
		sessorID = v.GetSensorId()
		if t.lastSeriesWriter != nil && (t.lastSessorId == sessorID) {
			dataSW = t.lastSeriesWriter
			ok = true
		} else {
			dataSW, ok = gd.dataSeriesWriters[sessorID]

			if !ok {
				//if not exist SeriesWriter, new it
				sensorDescriptor, bExistSensorDesc := schemaSensorDescriptorMap[sessorID]
				if !bExistSensorDesc {
					log.Error("input sensor is invalid: ", sessorID)
				} else {
					// new pagewriter
					pw, _ := NewPageWriter(sensorDescriptor)
					// new serieswrite
					dataSW, _ = NewSeriesWriter(strDeviceID, sensorDescriptor, pw, conf.PageSizeInByte)
					gd.dataSeriesWriters[sessorID] = dataSW
					ok = true
				}
			}
			t.lastSeriesWriter = dataSW
			t.lastSessorId = sessorID
		}
		if !ok {
			log.Error("time: %d, sensor id %s not found! ", timeST, sessorID)
		} else {
			//v.Write(t, dataSeriesWriter)
			if dataSW.GetTsDeviceId() == "" {
				log.Info("give seriesWriter is null, do nothing and return.")
			} else {
				//dataSW.Write(timeST, v)
				dataSW.time = timeST

				valueWriter = &(dataSW.valueWriter)

				valueWriter.timeEncoder.Encode(timeST, valueWriter.timeBuf)
				var valueInterface interface{} = v.value
				switch dataSW.tsDataType {
				case 0, 1, 2, 3, 4, 5:
					valueWriter.valueEncoder.Encode(valueInterface, valueWriter.valueBuf)
				default:
				}
				//log.CostWriteTimesTest1 += int64(time.Since(tsCurNew2))
				dataSW.valueCount++
				// statistics ignore here, if necessary, Statistics.java
				dataSW.pageStatistics.UpdateStats(valueInterface)

				if dataSW.minTimestamp == -1 {
					dataSW.minTimestamp = timeST
				}
				//tsCurNew2 = time.Now()
				// check page size and write page data to buffer
				//dataSW.checkPageSizeAndMayOpenNewpage()
				if dataSW.valueCount == conf.MaxNumberOfPointsInPage {
					dataSW.WritePage()
				} else if dataSW.valueCount >= dataSW.valueCountForNextSizeCheck {
					currentColumnSize := valueWriter.GetCurrentMemSize()
					if currentColumnSize > dataSW.psThres {
						// write data to buffer
						dataSW.WritePage()
					}
					dataSW.valueCountForNextSizeCheck = dataSW.psThres * 1.0 / currentColumnSize * dataSW.valueCount
				}
				//log.CostWriteTimesTest1 += int64(time.Since(tsCurNew2))
			}
		}
	}
	t.recordCount++
	return t.checkMemorySizeAndMayFlushGroup()
}

func (t *TsFileWriter) Close() bool {
	// finished write file, and write magic string at file tail
	//t.tsFileIoWriter.WriteMagic()
	//t.tsFileIoWriter.tsIoFile.Write([]byte("\n"))
	//t.tsFileIoWriter.tsIoFile.Close()

	t.CalculateMemSizeForAllGroup()
	t.flushAllRowGroups(false)
	t.tsFileIoWriter.EndFile(*t.schema)
	t.lastGroupDevice = nil
	t.lastSeriesWriter = nil
	t.lastSessorId = ""
	return true
}

func (t *TsFileWriter) checkMemorySizeAndMayFlushGroup() bool {
	if t.recordCount >= t.recordCountForNextMemCheck {
		memSize := t.CalculateMemSizeForAllGroup()
		if memSize >= t.rowGroupSizeThreshold {
			//log.Info("start write rowGroup, memory space occupy: %v", memSize)
			if t.oneRowMaxSize != 0 {
				t.recordCountForNextMemCheck = t.rowGroupSizeThreshold / int64(t.oneRowMaxSize)
			} //else{
			//	log.Info("tsFileWriter oneRowMaxSize is not correct.")
			//}

			return t.flushAllRowGroups(false)
		} else {
			if t.oneRowMaxSize != 0 {
				t.recordCountForNextMemCheck = t.recordCount + (t.rowGroupSizeThreshold-memSize)/int64(t.oneRowMaxSize)
			}
			return false
		}
	}
	return false
}

func (t *TsFileWriter) CalculateMemSizeForAllGroup() int64 {
	// calculate all group memory size
	var memTotalSize int64 = 0
	for _, v := range t.groupDevices {
		memTotalSize += v.UpdateMaxGroupMemSize()
	}

	// return max size for write rowGroupHeader
	return memTotalSize //128 * 1024 *1024
}

func (t *TsFileWriter) checkIsDeviceExist(tr *TsRecord, schema *fileSchema.FileSchema) (*RowGroupWriter, bool) {
	var groupDevice *RowGroupWriter
	var err error
	// check device
	//if _, ok := t.groupDevices[tr.GetDeviceId()]; !ok {
	var ok bool
	groupDevice, ok = t.groupDevices[tr.GetDeviceId()]
	if !ok {
		// if not exist
		groupDevice, err = NewRowGroupWriter(tr.GetDeviceId())
		if err != nil {
			log.Info("rowGroupWriter init ok!")
		}
		t.groupDevices[tr.GetDeviceId()] = groupDevice
		//} else { // if exist
		//	groupDevice = t.groupDevices[tr.GetDeviceId()]
	}
	schemaSensorDescriptorMap := schema.GetSensorDescriptiorMap()
	data := tr.GetDataPointSli()
	for _, v := range data {
		//if contain, _ := utils.MapContains(schemaSensorDescriptorMap, v.GetSensorId()); contain {
		//	//groupDevice.AddSeriesWriter(schemaSensorDescriptorMap[v.GetSensorId()], tsFileConf.PageSizeInByte)
		//	t.groupDevices[tr.GetDeviceId()].AddSeriesWriter(schemaSensorDescriptorMap[v.GetSensorId()], conf.PageSizeInByte)
		sensorDescriptor, bExistSensorDesc := schemaSensorDescriptorMap[v.GetSensorId()]
		if bExistSensorDesc {
			groupDevice.AddSeriesWriter(sensorDescriptor, conf.PageSizeInByte)
		} else {
			log.Error("input sensor is invalid: ", v.GetSensorId())
		}
		//log.Info("k=%v, v=%v\n", k, v)
	}
	return groupDevice, true
}

func NewTsFileWriter(file string) (*TsFileWriter, error) {
	// file schema
	fs, fsErr := fileSchema.New()
	if fsErr != nil {
		log.Error("init fileSchema failed.")
	}

	// tsFileIoWriter
	tfiWriter, tfiwErr := NewTsFileIoWriter(file)
	if tfiwErr != nil {
		log.Error("init tsFileWriter error = ", tfiwErr)
	}

	// write start magic
	tfiWriter.WriteMagic()

	// init rowGroupSizeThreshold
	var prgs int64 = int64(conf.GroupSizeInByte)
	rgst := int64(conf.GroupSizeInByte) - prgs

	return &TsFileWriter{
		tsFileIoWriter:             tfiWriter,
		schema:                     fs,
		recordCount:                0,
		recordCountForNextMemCheck: 1,
		primaryRowGroupSize:        prgs,
		rowGroupSizeThreshold:      rgst,
		groupDevices:               make(map[string]*RowGroupWriter),
	}, nil
}
