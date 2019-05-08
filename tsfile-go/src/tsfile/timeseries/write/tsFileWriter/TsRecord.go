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
 * @Package Name: tsRecord
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-27 下午2:36
 * @Description:
 */

import (
	"sync"
	"time"
)

type TsRecord struct {
	time     int64
	deviceId string
	//dataPointMap		map[string]*DataPoint
	DataPointSli []*DataPoint
	m            sync.Mutex
}

func (t *TsRecord) SetTime(time time.Time) {
	t.time = time.Unix()
	return
}

func (t *TsRecord) SetTimeDeviceID(time time.Time, dId string) {
	//PushBack(t, tuple)
	// t.dataPointMap[t.deviceId] = tuple
	//t.dataPointMap[t.deviceId] = tuple
	//t.DataPointSli = dataSlice
	t.time = time.Unix()
	t.deviceId = dId
	return
}

func (t *TsRecord) SetTimestampDeviceID(ts int64, dId string) {
	//PushBack(t, tuple)
	// t.dataPointMap[t.deviceId] = tuple
	//t.dataPointMap[t.deviceId] = tuple
	//t.DataPointSli = dataSlice
	t.time = ts
	t.deviceId = dId
	return
}

func (t *TsRecord) SetDataPointSli(dataSlice []*DataPoint) {
	//PushBack(t, tuple)
	// t.dataPointMap[t.deviceId] = tuple
	//t.dataPointMap[t.deviceId] = tuple
	t.DataPointSli = dataSlice
	return
}

func (t *TsRecord) AddTuple(tuple *DataPoint) {
	//PushBack(t, tuple)
	// t.dataPointMap[t.deviceId] = tuple
	//t.dataPointMap[t.deviceId] = tuple
	t.DataPointSli = append(t.DataPointSli, tuple)
	return
}

func (t *TsRecord) GetTime() int64 {
	return t.time
}

func (t *TsRecord) GetDeviceId() string {
	return t.deviceId
}

func (t *TsRecord) GetDataPointSli() []*DataPoint {
	return t.DataPointSli
}

//func PushBack(t *TsRecord, tuple dataPoint.DataPoint) {
//	//if tuple != nil {
//	//	return
//	//}
//	t.m.Lock()
//	defer t.m.Unlock()
//	t.DataPointList.PushBack(tuple)
//	return
//}
//
//func front(t *TsRecord) *list.Element {
//	t.m.Lock()
//	defer t.m.Unlock()
//	return t.DataPointList.Front()
//}
//
//func remove(t *TsRecord, element *list.Element) {
//	if element == nil {
//		return
//	}
//	t.m.Lock()
//	defer t.m.Unlock()
//	t.DataPointList.Remove(element)
//}

// this remove has some issue, we cann't use as the follow:
//for e := l.Front(); e != nil; e = e.Next {
//	l.Remove(e)
//}

// because when we remove ,element.next == nil then the loop for element != nil is ok,then exit.
// so we must use as the following two ways:
//way1:
//	var next *list.Element
//	for element := list.Front(); element != nil; element = next {
//		next = element.Next()
//		list.remove(element)
//	}
//
//way2:
//	for {
//		element := list.Front()
//		if element == nil {
//			break
//		}
//		list.remove(element)
//	}
//
//func len(t *TsRecord) int {
//	t.m.Lock()
//	defer t.m.Unlock()
//	return t.DataPointList.Len()
//}

func NewTsRecord(t time.Time, dId string) (*TsRecord, error) {
	return &TsRecord{
		time:     t.Unix(),
		deviceId: dId,
		//dataPointMap:make(map[string]*DataPoint),
		DataPointSli: make([]*DataPoint, 0),
	}, nil
}

func NewTsRecordUseTimestamp(t int64, dId string) (*TsRecord, error) {
	return &TsRecord{
		time:     t,
		deviceId: dId,
		//dataPointMap:make(map[string]*DataPoint),
		DataPointSli: make([]*DataPoint, 0),
	}, nil
}
