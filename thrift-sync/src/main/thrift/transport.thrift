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
namespace java org.apache.iotdb.service.transport.thrift
namespace py iotdb.thrift.transport

struct TransportStatus{
  1:required i32 code
  2:required string msg
}

// The sender and receiver need to check some info to confirm validity
struct IdentityInfo{
  // Check whether the ip of sender is in the white list of receiver.
  1:required string address

  // Sender needs to tell receiver its identity.
  2:required string pipeName
  3:required i64 createTime

  // The version of sender and receiver need to be the same.
  4:required string version

}

enum Type {
  TSFILE,
  DELETION,
  PHYSICALPLAN,
  FILE
}

enum RequestType {
  CREATE,
  START,
  STOP,
  DROP,
  HEARTBEAT
}

enum ResponseType {
  INFO,
  WARN,
  ERROR
}

struct MetaInfo{
  // The type of the pipeData in sending.
  1:required Type type

  // The name of the file in sending.
  2:required string fileName

  // The start index of the file slice in sending.
  3:required i64 startIndex
}

struct SyncRequest{
  1:required RequestType type
  2:required string pipeName
  3:required string remoteIp
  4:required i64 createTime
}

struct SyncResponse{
  1:required ResponseType type
  2:required string msg
}

service TransportService{
  TransportStatus handshake(IdentityInfo info);
  TransportStatus transportData(1:MetaInfo metaInfo, 2:binary buff, 3:binary digest);
  TransportStatus checkFileDigest(1:MetaInfo metaInfo, 2:binary digest);
  SyncResponse heartbeat(SyncRequest syncRequest)
}
