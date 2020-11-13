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
	"client-go/src/main/iotdbSession"
	"client-go/src/main/iotdbSession/utils"
	"testing"
)

var session iotdbSession.Session = iotdbSession.NewSession("127.0.0.1", "6667")

//TODO Modify the expected value to the query result
func TestSetStorageGroup(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		storageGroupId string
		expected       error
	}{
		{"root.sg1", nil},
		{"root.sg2", nil},
	} {
		if actually := session.SetStorageGroup(unit.storageGroupId); actually != unit.expected {
			t.Errorf("SetStorageGroup: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestDeleteStorageGroup(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		storageGroupId string
		expected       error
	}{
		{"root.sg1", nil},
	} {
		if actually := session.DeleteStorageGroup(unit.storageGroupId); actually != unit.expected {
			t.Errorf("DeleteStorageGroup: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestDeleteStorageGroups(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		storageGroupId []string
		expected       error
	}{
		{[]string{"root.sg2"}, nil},
	} {
		if actually := session.DeleteStorageGroups(unit.storageGroupId); actually != unit.expected {
			t.Errorf("DeleteStorageGroups: [%v], actually: [%v]", unit, actually)
		}
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
		expected   error
	}{
		{"root.sg1.dev1.status", utils.FLOAT, utils.PLAIN, utils.SNAPPY, nil},
	} {
		if actually := session.CreateTimeseries(unit.path, unit.dataType, unit.encoding, unit.compressor); actually != unit.expected {
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
		expected    error
	}{
		{[]string{"root.sg1.dev1.temperature"}, []int32{utils.FLOAT}, []int32{utils.PLAIN}, []int32{utils.SNAPPY}, nil},
	} {
		if actually := session.CreateMultiTimeseries(unit.paths, unit.dataTypes, unit.encodings, unit.compressors); actually != unit.expected {
			t.Errorf("CreateTimeseries: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestDeleteTimeseries(t *testing.T) {
	session.Open(false, 0)
	for _, unit := range []struct {
		paths    []string
		expected error
	}{
		{[]string{"root.sg1.dev1.status"}, nil},
	} {
		if actually := session.DeleteTimeseries(unit.paths); actually != unit.expected {
			t.Errorf("DeleteTimeseries: [%v], actually: [%v]", unit, actually)
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
		expected     error
	}{
		{"root.sg1.dev1", []string{"temperature"}, []string{"tem"}, 111, nil},
	} {
		if actually := session.InsertStringRecord(unit.deviceId, unit.measurements, unit.values, unit.timestamp); actually != unit.expected {
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
		expected  error
	}{
		{[]string{"root.sg1.dev1.temperature"}, 0, 111, nil},
	} {
		if actually := session.DeleteData(unit.paths, unit.startTime, unit.endTime); actually != unit.expected {
			t.Errorf("DeleteData: [%v], actually: [%v]", unit, actually)
		}
	}
	session.Close()
}

func TestGetTimeZone(t *testing.T) {
	session.Open(false, 0)
	if actually := session.GetTimeZone(); actually != iotdbSession.DefaultZoneId {
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
