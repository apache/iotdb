/*
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

namespace java org.apache.iotdb.protocol.influxdb.rpc.thrift

struct EndPoint {
  1: required string ip
  2: required i32 port
}

// The return status code and message in each response.
struct TSStatus {
  1: required i32 code
  2: optional string message
  3: optional list<TSStatus> subStatus
  4: optional EndPoint redirectNode
}

struct TSOpenSessionResp {
  1: required TSStatus status

  // Session id
  2: optional i64 sessionId

  // The configuration settings for this session.
  3: optional map<string, string> configuration
}

// OpenSession()
// Open a session (connection) on the server against which operations may be executed.
struct TSOpenSessionReq {
  2: required string zoneId
  3: optional string username
  4: optional string password
  5: optional map<string, string> configuration
}


// CloseSession()
// Closes the specified session and frees any resources currently allocated to that session.
// Any open operations in that session will be canceled.
struct TSCloseSessionReq {
  1: required i64 sessionId
}

// WritePoints()
// write points in influxdb
struct TSWritePointsReq{
  // The session to execute the statement against
  1: required i64 sessionId

  2: required string database
  3: optional string retentionPolicy
  4: optional string precision
  5: optional string consistency
  6: optional string lineProtocol
}

// CreateDatabase()
// create database in influxdb
struct TSCreateDatabaseReq{
  // The session to execute the statement against
  1: required i64 sessionId

  2: required string database
}

// WritePoints()
// write points in influxdb
struct TSQueryReq{
  // The session to execute the statement against
  1: required i64 sessionId

  2: required string command
  3: required string database
}

// WritePoints()
// write points in influxdb
struct TSQueryResultRsp{
  1: required TSStatus status

  2: required list<TSResult> results

  3: optional string error
}

struct TSResult{
  1: required list<TSSeries> series

  2: optional string error
}

struct TSSeries {
  1: required string name
  2: optional map<string,string> tags
  3: required list<string> columns

  4: required list<list<binary>> values
}

service InfluxDBService {
  TSOpenSessionResp openSession(1:TSOpenSessionReq req);

  TSStatus closeSession(1:TSCloseSessionReq req);

  TSStatus writePoints(1:TSWritePointsReq req);

  TSStatus createDatabase(1:TSCreateDatabaseReq req);

  TSQueryResultRsp query(1:TSQueryReq req);
}
