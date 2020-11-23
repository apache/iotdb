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

package test

import (
	"fmt"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client"
	"github.com/yanhongwangg/incubator-iotdb/client-go/src/main/client/utils"
	"strings"
	"testing"
)

var session = client.NewSession("127.0.0.1", "6667")

//TODO Modify the expected value to the query result
func TestSetStorageGroup(t *testing.T) {
	session.Open(false, 0)
	storageGroupIds := []string{"root.sg1", "root.sg2"}
	for i := 0; i < 2; i++ {
		var (
			actually = false
			expected = true
		)
		session.SetStorageGroup(storageGroupIds[i])
		sessionDataSet := session.ExecuteStatement("show storage group")
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if string(record.Fields[0].GetBinaryV()) == storageGroupIds[i] {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != expected {
			t.Errorf("SetStorageGroup: [%v], actually: [%v]", storageGroupIds[i], actually)
		}
	}
	session.Close()
}

func TestDeleteStorageGroup(t *testing.T) {
	session.Open(false, 0)
	var (
		actually       = true
		expected       = true
		storageGroupId = "root.sg1"
	)
	session.DeleteStorageGroup(storageGroupId)
	sessionDataSet := session.ExecuteStatement("show storage group")
	for {
		if sessionDataSet.HasNext() {
			record := sessionDataSet.Next()
			if string(record.Fields[0].GetBinaryV()) == "root.sg1" {
				actually = false
				break
			}
		} else {
			break
		}
	}
	if actually != expected {
		t.Errorf("DeleteStorageGroup: [%v], actually: [%v]", storageGroupId, actually)
	}
	session.Close()
}

func TestDeleteStorageGroups(t *testing.T) {
	session.Open(false, 0)
	var (
		actually        = true
		expected        = true
		storageGroupIds = []string{"root.sg2"}
	)
	session.DeleteStorageGroups(storageGroupIds)
	sessionDataSet := session.ExecuteStatement("show storage group")
	for {
		if sessionDataSet.HasNext() {
			record := sessionDataSet.Next()
			if string(record.Fields[0].GetBinaryV()) == "root.sg1" {
				actually = false
				break
			}
		} else {
			break
		}
	}
	sg := strings.Replace(strings.Trim(fmt.Sprint(storageGroupIds), "[]"), " ", ",", -1)
	if actually != expected {
		t.Errorf("DeleteStorageGroups: [%v], actually: [%v]", sg, actually)
	}
	session.Close()
}

func TestCreateTimeseries(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		path       string
		dataType   int32
		encoding   int32
		compressor int32
		expected   bool
	}{
		{"root.sg1.dev1.status", utils.FLOAT, utils.PLAIN, utils.SNAPPY, true},
	} {
		session.CreateTimeseries(unit.path, unit.dataType, unit.encoding, unit.compressor)
		if actually := session.CheckTimeseriesExists(unit.path); actually != unit.expected {
			t.Errorf("CreateTimeseries: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestCreateMultiTimeseries(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		paths       []string
		dataTypes   []int32
		encodings   []int32
		compressors []int32
		expected    bool
	}{
		{[]string{"root.sg1.dev1.temperature"}, []int32{utils.FLOAT}, []int32{utils.PLAIN}, []int32{utils.SNAPPY}, true},
	} {
		session.CreateMultiTimeseries(unit.paths, unit.dataTypes, unit.encodings, unit.compressors)
		for _, path := range unit.paths {
			if actually := session.CheckTimeseriesExists(path); actually != unit.expected {
				t.Errorf("CreateTimeseries: [%v], actually: [%v]", unit, actually)
			}
		}
	}
	session.Close()
}

func TestDeleteTimeseries(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		paths    []string
		expected bool
	}{
		{[]string{"root.sg1.dev1.status"}, false},
	} {
		session.DeleteTimeseries(unit.paths)
		for _, path := range unit.paths {
			if actually := session.CheckTimeseriesExists(path); actually != unit.expected {
				t.Errorf("DeleteTimeseries: [%v], actually: [%v]", unit, actually)
			}
		}
	}
	session.Close()
}

func TestInsertStringRecord(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		deviceId     string
		measurements []string
		values       []string
		timestamp    int64
		expected     bool
	}{
		{"root.sg1.dev1", []string{"s"}, []string{"tem"}, 111, true},
	} {
		session.InsertStringRecord(unit.deviceId, unit.measurements, unit.values, unit.timestamp)
		sessionDataSet := session.ExecuteStatement("select count(s) from root.sg1.dev1")
		var actually = false
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if record.Fields[0].GetLongV() == 1 {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("InsertStringRecord: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestDeleteData(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		paths     []string
		startTime int64
		endTime   int64
		expected  bool
	}{
		{[]string{"root.sg1.dev1.temperature"}, 0, 111, true},
	} {
		session.DeleteData(unit.paths, unit.startTime, unit.endTime)
		sessionDataSet := session.ExecuteStatement("select count(temperature) from root.sg1.dev1")
		var actually = false
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if record.Fields[0].GetLongV() == 0 {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("DeleteData: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestGetTimeZone(t *testing.T) {
	session.Open(false, 0)
	if actually := session.GetTimeZone(); actually != client.DefaultZoneId {
		t.Errorf("GetTimeZone, actually: [%v]", actually)
	}
	session.Close()
}

func TestSetTimeZone(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		timeZone string
		expected string
	}{
		{"GMT", "GMT"},
	} {
		session.SetTimeZone(unit.timeZone)
		if actually := session.GetTimeZone(); actually != unit.expected {
			t.Errorf("SetTimeZone: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestInsertRecord(t *testing.T) {
	session.Open(false, 0)
	var val int32 = 2
	for _, unit := range []struct {
		deviceId     string
		measurements []string
		dataTypes    []int32
		values       []interface{}
		timestamp    int64
		expected     bool
	}{
		{"root.sg1.dev1", []string{"wind"}, []int32{utils.INT32},
			[]interface{}{val}, 111, true},
	} {
		session.InsertRecord(unit.deviceId, unit.measurements, unit.dataTypes, unit.values, unit.timestamp)
		sessionDataSet := session.ExecuteStatement("select count(wind) from root.sg1.dev1")
		var actually = false
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if record.Fields[0].GetLongV() == 1 {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("InsertRecord: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestInsertRecords(t *testing.T) {
	session.Open(false, 0)
	var val int32 = 2
	for _, unit := range []struct {
		deviceIds    []string
		measurements [][]string
		dataTypes    [][]int32
		values       [][]interface{}
		timestamps   []int64
		expected     bool
	}{
		{[]string{"root.sg1.dev1"}, [][]string{{"height"}}, [][]int32{{utils.INT32}},
			[][]interface{}{{val}}, []int64{222}, true},
	} {
		session.InsertRecords(unit.deviceIds, unit.measurements, unit.dataTypes, unit.values, unit.timestamps)
		sessionDataSet := session.ExecuteStatement("select count(height) from root.sg1.dev1")
		var actually = false
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if record.Fields[0].GetLongV() == 1 {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("InsertRecords: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestInsertTablet(t *testing.T) {
	var (
		deviceId     = "root.sg1.dev1"
		measurements = []string{"s1", "s2"}
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
	session.Open(false, 0)
	for _, unit := range []struct {
		tablet   utils.Tablet
		expected bool
	}{
		{tablet, true},
	} {
		session.InsertTablet(unit.tablet)
		sessionDataSet := session.ExecuteStatement("select count(s2) from root.sg1.dev1")
		var actually = false
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if record.Fields[0].GetLongV() == 2 {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("InsertTablet: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestInsertTablets(t *testing.T) {
	var (
		deviceId1     = "root.sg1.dev1"
		measurements1 = []string{"s3", "s4"}
		dataTypes1    = []int32{utils.TEXT, utils.INT32}
		values1       = make([]interface{}, 2)
		timestamp1    = []int64{154, 123}
	)
	values1[0] = []string{"aaa", "bbb"}
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
	session.Open(false, 0)
	for _, unit := range []struct {
		tablets  []utils.Tablet
		expected bool
	}{
		{tablets, true},
	} {
		session.InsertTablets(unit.tablets)
		sessionDataSet := session.ExecuteQueryStatement("select count(s3) from root.sg1.dev1")
		var actually = false
		for {
			if sessionDataSet.HasNext() {
				record := sessionDataSet.Next()
				if record.Fields[0].GetLongV() == 2 {
					actually = true
					break
				}
			} else {
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("InsertTablets: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestExecuteStatement(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		sql      string
		expected bool
	}{
		{"create timeseries root.ln.wf02.wt02.s5 with datatype=BOOLEAN,encoding=PLAIN", true},
	} {
		session.ExecuteStatement(unit.sql)
		if actually := session.CheckTimeseriesExists("root.ln.wf02.wt02.s5"); actually != unit.expected {
			t.Errorf("ExecuteStatement: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestExecuteRawDataQuery(t *testing.T) {
	session.Open(false, 0)
	session.ExecuteUpdateStatement("insert into root.ln.wf02.wt02(time,s5) values(1,true)")
	var start int64 = 1
	var end int64 = 2
	for _, unit := range []struct {
		paths     []string
		startTime int64
		endTime   int64
		expected  bool
	}{
		{[]string{"root.ln.wf02.wt02.s5"}, start, end, true},
	} {
		sessionDataSet := session.ExecuteRawDataQuery(unit.paths, unit.startTime, unit.endTime)
		count := 0
		actually := false
		for {
			if sessionDataSet.HasNext() {
				count++
			} else {
				if count == 1 {
					actually = true
				}
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("ExecuteRawDataQuery: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestExecuteBatchStatement(t *testing.T) {
	session.Open(false, 0)
	var statements = []string{"insert into root.ln.wf02.wt02(time,s5) values(1,true)",
		"insert into root.ln.wf02.wt02(time,s5) values(2,true)"}
	for _, unit := range []struct {
		sqls     []string
		expected bool
	}{
		{statements, true},
	} {
		session.ExecuteBatchStatement(unit.sqls)
		sessionDataSet := session.ExecuteQueryStatement("select s5 from root.ln.wf02.wt02")
		count := 0
		actually := false
		for {
			if sessionDataSet.HasNext() {
				count++
			} else {
				if count == 2 {
					actually = true
				}
				break
			}
		}
		if actually != unit.expected {
			t.Errorf("ExecuteBatchStatement: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}
