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

namespace java org.apache.iotdb.mpp.rpc.thrift


struct FragmentInstanceId {
  1: required string queryId
  2: required string fragmentId
  3: required string instanceId
}

struct GetDataBlockReqest {
  1: required FragmentInstanceId fragnemtInstanceId
  2: required i64 blockId
}

struct GetDataBlockResponse {
  1: required list<binary> tsBlocks
}

struct NewDataBlockEvent {
  1: required FragmentInstanceId fragmentInstanceId
  2: required string operatorId
  3: required i64 blockId
}

struct EndOfDataBlockEvent {
  1: required FragmentInstanceId fragmentInstanceId
  2: required string operatorId
}

service DataBlockService {
  GetDataBlockResponse getDataBlock(GetDataBlockReqest req);

  void onNewDataBlockEvent(NewDataBlockEvent e);

  void onEndOfDataBlockEvent(EndOfDataBlockEvent e);
}
