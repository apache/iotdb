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

enum TSProtocolVersion {
  IOTDB_SERVICE_PROTOCOL_V1,
  IOTDB_SERVICE_PROTOCOL_V2,//V2 is the first version that we can check version compatibility
  IOTDB_SERVICE_PROTOCOL_V3,//V3 is incompatible with V2
}

struct TSOpenSessionResp {
  1: required TSStatus status

  // The protocol version that the server is using.
  2: required TSProtocolVersion serverProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1

  // Session id
  3: optional i64 sessionId

  // The configuration settings for this session.
  4: optional map<string, string> configuration
}

// OpenSession()
// Open a session (connection) on the server against which operations may be executed.
struct TSOpenSessionReq {
  1: required TSProtocolVersion client_protocol = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
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
  1: required string database,
  2: required string retentionPolicy,
  3: required string precision,
  4: required string consistency,
  5: required string lineProtocol,
}

service InfluxDBService {
  TSOpenSessionResp openSession(1:TSOpenSessionReq req);

  TSStatus closeSession(1:TSCloseSessionReq req);

  TSStatus writePoints(1:TSWritePointsReq req);
}
