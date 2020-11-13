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

var emptyResultSet bool = false

type IoTDBRpcDataSet struct {
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
	FetchSize            int32
}

func (ioTDBRpcDataSet *IoTDBRpcDataSet) HasCachedResults() bool {
	return false
}
func (ioTDBRpcDataSet *IoTDBRpcDataSet) ConstructOneRow() bool {
	return false
}

func (ioTDBRpcDataSet *IoTDBRpcDataSet) FetchResults() bool {
	return false
}

func (ioTDBRpcDataSet *IoTDBRpcDataSet) Next() bool {
	if ioTDBRpcDataSet.HasCachedResults() {
		ioTDBRpcDataSet.ConstructOneRow()
		return true
	}
	if emptyResultSet {
		return false
	}
	if ioTDBRpcDataSet.FetchResults() {
		ioTDBRpcDataSet.ConstructOneRow()
		return true
	}
	return false
}
