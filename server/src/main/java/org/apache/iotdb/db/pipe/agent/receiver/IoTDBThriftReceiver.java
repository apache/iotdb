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

package org.apache.iotdb.db.pipe.core.connector.impl.iotdb;

import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.service.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

public interface IoTDBThriftReceiver {
  TPipeHandshakeResp handleHandshakeReq(TPipeHandshakeReq req);

  TPipeHeartbeatResp handleHeartbeatReq(TPipeHeartbeatReq req);

  TPipeTransferResp handleTransferReq(
      TPipeTransferReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher);

  void handleExit();
}
