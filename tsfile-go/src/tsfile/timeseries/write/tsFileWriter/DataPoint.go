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
 * @Package Name: dataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-28 下午4:27
 * @Description:
 */

import (
	"sync"
	_ "tsfile/common/constant"
	"tsfile/common/log"
)

type DataPointOperate interface {
	write()
}

type DataPoint struct {
	sensorId string
	//tsDataType constant.TSDataType
	value interface{}
}

func (d *DataPoint) GetSensorId() string {
	return d.sensorId
}

func (d *DataPoint) Write(t int64, sw *SeriesWriter) bool {
	if sw.GetTsDeviceId() == "" {
		log.Info("give seriesWriter is null, do nothing and return.")
		return false
	}
	sw.Write(t, d)
	return true
}

func (d *DataPoint) SetValue(sId string, val interface{}) {
	d.sensorId = sId
	d.value = val
}

var dataPointMutex sync.Mutex
var dataPointArrBuf []DataPoint //= make([]FloatDataPoint, 100)
var dataPointArrBufCount int = 0

func getDataPoint() *DataPoint {
	dataPointMutex.Lock()
	if dataPointArrBufCount == 0 {
		dataPointArrBufCount = 200
		dataPointArrBuf = make([]DataPoint, dataPointArrBufCount)
	}
	dataPointArrBufCount--
	f := &(dataPointArrBuf[dataPointArrBufCount])
	dataPointMutex.Unlock()
	return f
}

func NewDataPoint() (*DataPoint, error) {
	return &DataPoint{}, nil
}
