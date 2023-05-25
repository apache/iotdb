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

struct InfluxEndPoint {
  1: required string ip
  2: required i32 port
}

// The return status code and message in each response.
struct InfluxTSStatus {
  1: required i32 code
  2: optional string message
  3: optional list<InfluxTSStatus> subStatus
  4: optional InfluxEndPoint redirectNode
}

struct InfluxOpenSessionResp {
  1: required InfluxTSStatus status

  // Session id
  2: optional i64 sessionId

  // The configuration settings for this session.
  3: optional map<string, string> configuration
}

// OpenSession()
// Open a session (connection) on the server against which operations may be executed.
struct InfluxOpenSessionReq {
  2: required string zoneId
  3: required string username
  4: optional string password
  5: optional map<string, string> configuration
}


// CloseSession()
// Closes the specified session and frees any resources currently allocated to that session.
// Any open operations in that session will be canceled.
struct InfluxCloseSessionReq {
  1: required i64 sessionId
}

// WritePoints()
// write points in influxdb
struct InfluxWritePointsReq{
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
struct InfluxCreateDatabaseReq{
  // The session to execute the statement against
  1: required i64 sessionId

  2: required string database
}

// query()
// query in influxdb
struct InfluxQueryReq{
  // The session to execute the statement against
  1: required i64 sessionId

  2: required string command
  3: required string database
}

struct InfluxQueryResultRsp{
  1: required InfluxTSStatus status

  2: optional string resultJsonString

}

service InfluxDBService {
  InfluxOpenSessionResp openSession(1:InfluxOpenSessionReq req);

  InfluxTSStatus closeSession(1:InfluxCloseSessionReq req);

  InfluxTSStatus writePoints(1:InfluxWritePointsReq req);

  InfluxTSStatus createDatabase(1:InfluxCreateDatabaseReq req);

  InfluxQueryResultRsp query(1:InfluxQueryReq req);
}
