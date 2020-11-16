/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package utils

import (
	"bytes"
	"context"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"github.com/yanhongwangg/go-thrift/rpc"
)

type IoTDBRpcDataSet struct {
	Sql                        string
	ColumnNameList             []string
	ColumnTypeList             []string
	ColumnNameIndex            map[string]int32
	QueryId                    int64
	SessionId                  int64
	IgnoreTimeStamp            bool
	Client                     *rpc.TSIServiceClient
	QueryDataSet               *rpc.TSQueryDataSet
	FetchSize                  int32
	emptyResultSet             bool
	isClosed                   bool
	time                       []byte
	rowsIndex                  int
	currentBitmap              []byte
	ColumnTypeDeduplicatedList []string
	Values                     [][]byte
	HasCachedRecord            bool
	ColumnOrdinalMap           map[string]int32
	columnSize                 int
}

const (
	Flag         = 0x80
	StarIndex    = 2
	TimeStampStr = "Time"
)

func (r *IoTDBRpcDataSet) init() {
	r.emptyResultSet = false
	r.isClosed = false
	r.rowsIndex = 0
	r.HasCachedRecord = false
	r.ColumnOrdinalMap = make(map[string]int32)
}

func NewIoTDBRpcDataSet(dataSet *SessionDataSet, fetchSize int32) *IoTDBRpcDataSet {
	var ioTDBRpcDataSet = IoTDBRpcDataSet{
		Sql:             dataSet.Sql,
		QueryId:         dataSet.QueryId,
		SessionId:       dataSet.SessionId,
		QueryDataSet:    dataSet.QueryDataSet,
		IgnoreTimeStamp: dataSet.IgnoreTimeStamp,
		Client:          dataSet.Client,
		FetchSize:       fetchSize,
	}
	ioTDBRpcDataSet.init()
	ioTDBRpcDataSet.columnSize = len(dataSet.ColumnNameList)
	if !ioTDBRpcDataSet.IgnoreTimeStamp {
		ioTDBRpcDataSet.ColumnNameList = append(ioTDBRpcDataSet.ColumnNameList, TimeStampStr)
		ioTDBRpcDataSet.ColumnTypeList = append(ioTDBRpcDataSet.ColumnTypeList, "INT64")
		ioTDBRpcDataSet.ColumnOrdinalMap[TimeStampStr] = 1
	}
	if dataSet.ColumnNameIndex != nil {
		ioTDBRpcDataSet.ColumnTypeDeduplicatedList = make([]string, len(dataSet.ColumnNameIndex))
		for i := 0; i < len(dataSet.ColumnNameList); i++ {
			name := dataSet.ColumnNameList[i]
			ioTDBRpcDataSet.ColumnNameList = append(ioTDBRpcDataSet.ColumnNameList, name)
			ioTDBRpcDataSet.ColumnTypeList = append(ioTDBRpcDataSet.ColumnTypeList, dataSet.ColumnTypeList[i])
			if _, ok := ioTDBRpcDataSet.ColumnOrdinalMap[name]; !ok {
				index := dataSet.ColumnNameIndex[name]
				ioTDBRpcDataSet.ColumnOrdinalMap[name] = index + StarIndex
				ioTDBRpcDataSet.ColumnTypeDeduplicatedList[index] = dataSet.ColumnTypeList[i]
			}
		}
	} else {
		ioTDBRpcDataSet.ColumnTypeDeduplicatedList = []string{}
		index := StarIndex
		for i := 0; i < len(dataSet.ColumnNameList); i++ {
			name := dataSet.ColumnNameList[i]
			ioTDBRpcDataSet.ColumnNameList = append(ioTDBRpcDataSet.ColumnNameList, name)
			ioTDBRpcDataSet.ColumnTypeList = append(ioTDBRpcDataSet.ColumnTypeList, dataSet.ColumnTypeList[i])
			if _, ok := ioTDBRpcDataSet.ColumnOrdinalMap[name]; !ok {
				ioTDBRpcDataSet.ColumnOrdinalMap[name] = int32(index)
				index++
				ioTDBRpcDataSet.ColumnTypeDeduplicatedList = append(ioTDBRpcDataSet.ColumnTypeDeduplicatedList,
					dataSet.ColumnTypeList[i])
			}
		}
	}
	ioTDBRpcDataSet.time = make([]byte, 8)
	ioTDBRpcDataSet.currentBitmap = make([]byte, len(ioTDBRpcDataSet.ColumnTypeDeduplicatedList))
	ioTDBRpcDataSet.Values = make([][]byte, len(ioTDBRpcDataSet.ColumnTypeDeduplicatedList))
	for i := 0; i < len(ioTDBRpcDataSet.Values); i++ {
		dataType := ioTDBRpcDataSet.ColumnTypeDeduplicatedList[i]
		switch dataType {
		case "BOOLEAN":
			ioTDBRpcDataSet.Values[i] = make([]byte, 1)
			break
		case "INT32":
			ioTDBRpcDataSet.Values[i] = make([]byte, 4)
			break
		case "INT64":
			ioTDBRpcDataSet.Values[i] = make([]byte, 8)
			break
		case "FLOAT":
			ioTDBRpcDataSet.Values[i] = make([]byte, 4)
			break
		case "DOUBLE":
			ioTDBRpcDataSet.Values[i] = make([]byte, 8)
			break
		case "TEXT":
			ioTDBRpcDataSet.Values[i] = nil
			break
		}
	}
	return &ioTDBRpcDataSet
}

func (r *IoTDBRpcDataSet) hasCachedResults() bool {
	return r.QueryDataSet != nil && len(r.QueryDataSet.Time) != 0
}

func (r *IoTDBRpcDataSet) getColumnSize() int {
	return r.columnSize
}

func (r *IoTDBRpcDataSet) constructOneRow() {
	r.time = r.QueryDataSet.Time[:8]
	r.QueryDataSet.Time = r.QueryDataSet.Time[8:]
	for i := 0; i < len(r.QueryDataSet.BitmapList); i++ {
		bitmapBuffer := r.QueryDataSet.BitmapList[i]
		if r.rowsIndex%8 == 0 {
			r.currentBitmap[i] = bitmapBuffer[0]
			r.QueryDataSet.BitmapList[i] = bitmapBuffer[1:]
		}
		if !r.isNull(int32(i), r.rowsIndex) {
			valueBuffer := r.QueryDataSet.ValueList[i]
			dataType := r.ColumnTypeDeduplicatedList[i]
			switch dataType {
			case "BOOLEAN":
				r.Values[i] = valueBuffer[:1]
				r.QueryDataSet.ValueList[i] = valueBuffer[1:]
			case "INT32":
				r.Values[i] = valueBuffer[:4]
				r.QueryDataSet.ValueList[i] = valueBuffer[4:]
			case "INT64":
				r.Values[i] = valueBuffer[:8]
				r.QueryDataSet.ValueList[i] = valueBuffer[8:]
			case "FLOAT":
				r.Values[i] = valueBuffer[:4]
				r.QueryDataSet.ValueList[i] = valueBuffer[4:]
			case "DOUBLE":
				r.Values[i] = valueBuffer[:8]
				r.QueryDataSet.ValueList[i] = valueBuffer[8:]
			case "TEXT":
				buf := bytes.NewBuffer(valueBuffer[:4])
				var tmp uint32
				binary.Read(buf, binary.BigEndian, &tmp)
				length := int(tmp)
				r.Values[i] = valueBuffer[4 : 4+length]
				r.QueryDataSet.ValueList[i] = valueBuffer[4+length:]
			default:
				log.Error("not support dataType")
			}
		}
	}
	r.rowsIndex++
	r.HasCachedRecord = true
}

func (r *IoTDBRpcDataSet) isNull(index int32, rowNum int) bool {
	bitmap := r.currentBitmap[index]
	shift := rowNum % 8
	return ((Flag >> shift) & (bitmap & 0xff)) == 0
}

func (r *IoTDBRpcDataSet) isNil(columnIndex int) bool {
	index := r.ColumnOrdinalMap[r.findColumnNameByIndex(columnIndex)] - StarIndex
	if index < 0 {
		return true
	}
	return r.isNull(index, r.rowsIndex-1)
}

func (r *IoTDBRpcDataSet) findColumnNameByIndex(columnIndex int) string {
	if columnIndex <= 0 {
		log.Error("column index should start from 1")
	}
	if columnIndex > len(r.ColumnNameList) {
		log.Error("column index out of range")
	}
	return r.ColumnNameList[columnIndex-1]
}

func (r *IoTDBRpcDataSet) fetchResults() bool {
	r.rowsIndex = 0
	request := rpc.TSFetchResultsReq{
		SessionId: r.SessionId,
		Statement: r.Sql,
		FetchSize: r.FetchSize,
		QueryId:   r.QueryId,
		IsAlign:   true,
	}
	resp, _ := r.Client.FetchResults(context.Background(), &request)
	if !resp.HasResultSet {
		r.emptyResultSet = true
	} else {
		r.QueryDataSet = resp.GetQueryDataSet()
	}
	return resp.HasResultSet
}

func (r *IoTDBRpcDataSet) close() {
	if r.isClosed {
		return
	}
	if r.Client != nil {
		closeReq := rpc.TSCloseOperationReq{SessionId: r.SessionId, QueryId: &r.QueryId}
		r.Client.CloseOperation(context.Background(), &closeReq)
	}
	r.Client = nil
	r.isClosed = true
}

func (r *IoTDBRpcDataSet) next() bool {
	if r.hasCachedResults() {
		r.constructOneRow()
		return true
	}
	if r.emptyResultSet {
		return false
	}
	if r.fetchResults() {
		r.constructOneRow()
		return true
	}
	return false
}
