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

import "client-go/src/main/iotdbSession/utils"
import "client-go/src/main/iotdbSession"

var session iotdbSession.Session

func main() {
	session = iotdbSession.NewSession("127.0.0.1", "6667")
	session.Open(false, 0)
	setStorageGroup()
	deleteStorageGroup()
	deleteStorageGroups()
	createTimeseries()
	createMultiTimeseries()
	deleteTimeseries()
	insertStringRecord()
	deleteData()
	setTimeZone()
	println(getTimeZone())
	/*session.InsertRecord("root.ln.wf02.wt02",[]string{"hardware"},[]int32{1},
	[]interface{}{3,"123"},222)*/
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
	var path = "root.sg1.dev1.status"
	var dataType = utils.FLOAT
	var encoding = utils.PLAIN
	var compressor = utils.SNAPPY
	session.CreateTimeseries(path, dataType, encoding, compressor)
}

func createMultiTimeseries() {
	var paths = []string{"root.sg1.dev1.temperature"}
	var dataTypes = []int32{utils.TEXT}
	var encodings = []int32{utils.PLAIN}
	var compressors = []int32{utils.SNAPPY}
	session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors)
}

func deleteTimeseries() {
	var paths = []string{"root.sg1.dev1.status"}
	session.DeleteTimeseries(paths)
}

func insertStringRecord() {
	var deviceId = "root.ln.wf02.wt02"
	var measurements = []string{"hardware"}
	var values = []string{"123"}
	var timestamp int64 = 12
	session.InsertStringRecord(deviceId, measurements, values, timestamp)
}

func deleteData() {
	var paths = []string{"root.sg1.dev1.status"}
	var startTime int64 = 0
	var endTime int64 = 12
	session.DeleteData(paths, startTime, endTime)
}

func setTimeZone() () {
	var timeZone = "GMT"
	session.SetTimeZone(timeZone)
}

func getTimeZone() string {
	return session.GetTimeZone()
}
