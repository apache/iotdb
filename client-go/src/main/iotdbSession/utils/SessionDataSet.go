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

import "github.com/yanhongwangg/go-thrift/rpc"

var sessionDataSet *SessionDataSet
var defaultFetchSize int32 = 10000
var ioTDBRpcDataSet IoTDBRpcDataSet /*= IoTDBRpcDataSet{Sql: sessionDataSet.Sql,ColumnTypeList: sessionDataSet.ColumnTypeList,
ColumnNameList: sessionDataSet.ColumnNameList,ColumnNameIndex: sessionDataSet.ColumnNameIndex,QueryId: sessionDataSet.QueryId,
SessionId: sessionDataSet.SessionId,IgnoreTimeStamp: sessionDataSet.IgnoreTimeStamp,Client: sessionDataSet.Client,
QueryDataSet: sessionDataSet.QueryDataSet,NonAlignQueryDataSet: sessionDataSet.NonAlignQueryDataSet,FetchSize: defaultFetchSize}*/
type SessionDataSet struct {
	Sql                  string
	ColumnNameList       []string
	ColumnTypeList       []string
	ColumnNameIndex      map[string]int32
	QueryId              int64
	SessionId            string
	IgnoreTimeStamp      bool
	Client               *rpc.TSIServiceClient
	QueryDataSet         *rpc.TSQueryDataSet
	NonAlignQueryDataSet *rpc.TSQueryNonAlignDataSet
}

func (sessionDataSet *SessionDataSet) GetFetchSize() int32 {

	return ioTDBRpcDataSet.FetchSize
}

func (sessionDataSet *SessionDataSet) SetFetchSize(fetchSize int32) {
	ioTDBRpcDataSet.FetchSize = fetchSize
}

func (sessionDataSet *SessionDataSet) GetColumnNames() []string {
	return ioTDBRpcDataSet.ColumnNameList
}

func (sessionDataSet *SessionDataSet) GetColumnTypes() []string {
	return ioTDBRpcDataSet.ColumnTypeList
}

func (sessionDataSet *SessionDataSet) hasNext() bool {
	return ioTDBRpcDataSet.Next()
}
