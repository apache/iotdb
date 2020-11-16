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

package main

import (
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client/utils"
)

var session client.Session

func main() {
	session = client.NewSession("127.0.0.1", "6667")
	session.Open(false, 0)
	setStorageGroup()
	deleteStorageGroup()
	deleteStorageGroups()
	createTimeseries()
	createMultiTimeseries()
	deleteTimeseries()
	insertStringRecord()
	insertRecord()
	insertRecords()
	insertTablet()
	insertTablets()
	deleteData()
	setTimeZone()
	println(getTimeZone())
	executeStatement()
	executeQueryStatement()
	executeRawDataQuery()
	executeBatchStatement()
	session.Close()
}

func setStorageGroup() {
	var storageGroupId = "root.ln1"
	session.SetStorageGroup(storageGroupId)
}

func deleteStorageGroup() {
	var storageGroupId = "root.ln1"
	session.DeleteStorageGroup(storageGroupId)
}

func deleteStorageGroups() {
	var storageGroupId = []string{"root.ln1"}
	session.DeleteStorageGroups(storageGroupId)
}

func createTimeseries() {
	var (
		path       = "root.sg1.dev1.status"
		dataType   = utils.FLOAT
		encoding   = utils.PLAIN
		compressor = utils.SNAPPY
	)
	session.CreateTimeseries(path, dataType, encoding, compressor)
}

func createMultiTimeseries() {
	var (
		paths       = []string{"root.sg1.dev1.temperature"}
		dataTypes   = []int32{utils.TEXT}
		encodings   = []int32{utils.PLAIN}
		compressors = []int32{utils.SNAPPY}
	)
	session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors)
}

func deleteTimeseries() {
	var paths = []string{"root.sg1.dev1.status"}
	session.DeleteTimeseries(paths)
}

func insertStringRecord() {
	var (
		deviceId           = "root.ln.wf02.wt02"
		measurements       = []string{"hardware"}
		values             = []string{"123"}
		timestamp    int64 = 12
	)
	session.InsertStringRecord(deviceId, measurements, values, timestamp)
}

func insertRecord() {
	var (
		deviceId           = "root.sg1.dev1"
		measurements       = []string{"status"}
		values             = []interface{}{"123"}
		dataTypes          = []int32{utils.TEXT}
		timestamp    int64 = 12
	)
	session.InsertRecord(deviceId, measurements, dataTypes, values, timestamp)
}

func insertRecords() {
	var (
		deviceId     = []string{"root.sg1.dev1"}
		measurements = [][]string{{"status"}}
		dataTypes    = [][]int32{{utils.TEXT}}
		values       = [][]interface{}{{"123"}}
		timestamp    = []int64{12}
	)
	session.InsertRecords(deviceId, measurements, dataTypes, values, timestamp)
}

func deleteData() {
	var (
		paths           = []string{"root.sg1.dev1.status"}
		startTime int64 = 0
		endTime   int64 = 12
	)
	session.DeleteData(paths, startTime, endTime)
}

func insertTablet() {
	var (
		deviceId     = "root.sg1.dev1"
		measurements = []string{"status", "tem"}
		dataTypes    = []int32{utils.INT32, utils.INT32}
		values       = make([]interface{}, 2)
		timestamp    = []int64{154, 123}
	)
	values[0] = []int32{777, 6666}
	values[1] = []int32{888, 999}
	var tablet = utils.Tablet{
		DeviceId:     deviceId,
		Measurements: measurements,
		Values:       values,
		Timestamps:   timestamp,
		Types:        dataTypes,
	}
	session.InsertTablet(tablet)
}

func insertTablets() {
	var (
		deviceId1     = "root.sg1.dev1"
		measurements1 = []string{"status", "tem"}
		dataTypes1    = []int32{utils.INT32, utils.INT32}
		values1       = make([]interface{}, 2)
		timestamp1    = []int64{154, 123}
	)
	values1[0] = []int32{777, 6666}
	values1[1] = []int32{888, 999}
	var tablet1 = utils.Tablet{
		DeviceId:     deviceId1,
		Measurements: measurements1,
		Values:       values1,
		Timestamps:   timestamp1,
		Types:        dataTypes1,
	}
	var (
		deviceId2     = "root.sg1.dev2"
		measurements2 = []string{"status", "tem"}
		dataTypes2    = []int32{utils.INT32, utils.INT32}
		values2       = make([]interface{}, 2)
		timestamp2    = []int64{154, 123}
	)
	values2[0] = []int32{777, 6666}
	values2[1] = []int32{888, 999}
	var tablet2 = utils.Tablet{
		DeviceId:     deviceId2,
		Measurements: measurements2,
		Values:       values2,
		Timestamps:   timestamp2,
		Types:        dataTypes2,
	}
	tablets := []utils.Tablet{tablet1, tablet2}
	session.InsertTablets(tablets)
}

func setTimeZone() {
	var timeZone = "GMT"
	session.SetTimeZone(timeZone)
}

func getTimeZone() string {
	return session.GetTimeZone()
}

func executeStatement() {
	var sql = "show storage group"
	sessionDataSet := session.ExecuteStatement(sql)
	for i := 0; i < len(sessionDataSet.GetColumnNames()); i++ {
		println(sessionDataSet.GetColumnNames()[i])
	}
	for {
		if sessionDataSet.HasNext() {
			record := sessionDataSet.Next()
			for i := 0; i < len(record.Fields); i++ {
				println(record.Fields[i].GetStringValue())
			}
		} else {
			break
		}
	}
}

func executeQueryStatement() {
	var sql = "select count(s3) from root.sg1.dev1"
	sessionDataSet := session.ExecuteQueryStatement(sql)
	for i := 0; i < len(sessionDataSet.GetColumnNames()); i++ {
		println(sessionDataSet.GetColumnNames()[i])
	}
	for {
		if sessionDataSet.HasNext() {
			record := sessionDataSet.Next()
			for i := 0; i < len(record.Fields); i++ {
				println(record.Fields[i].GetLongV())
			}
		} else {
			break
		}
	}
}

func executeRawDataQuery() {
	session.ExecuteUpdateStatement("insert into root.ln.wf02.wt02(time,s5) values(1,true)")
	var (
		paths     []string = []string{"root.ln.wf02.wt02.s5"}
		startTime int64    = 1
		endTime   int64    = 200
	)
	sessionDataSet := session.ExecuteRawDataQuery(paths, startTime, endTime)
	for i := 0; i < len(sessionDataSet.GetColumnNames()); i++ {
		println(sessionDataSet.GetColumnNames()[i])
	}
	for {
		if sessionDataSet.HasNext() {
			record := sessionDataSet.Next()
			println(record.Timestamp)
			for i := 0; i < len(record.Fields); i++ {
				switch record.Fields[i].DataType {
				case "BOOLEAN":
					println(record.Fields[i].GetBoolV())
					break
				case "INT32":
					println(record.Fields[i].GetIntV())
					break
				case "INT64":
					println(record.Fields[i].GetLongV())
					break
				case "FLOAT":
					println(record.Fields[i].GetFloatV())
					break
				case "DOUBLE":
					println(record.Fields[i].GetDoubleV())
					break
				case "TEXT":
					println(string(record.Fields[i].GetBinaryV()))
					break
				}

			}
		} else {
			break
		}
	}
}

func executeBatchStatement() {
	var sqls = []string{"insert into root.ln.wf02.wt02(time,s5) values(1,true)",
		"insert into root.ln.wf02.wt02(time,s5) values(2,true)"}
	session.ExecuteBatchStatement(sqls)
}
