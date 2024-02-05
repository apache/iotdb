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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.client.IoTDBThriftSyncLeaderCacheClientManager;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IoTDBThriftSyncClientDataNodeManager extends IoTDBThriftSyncLeaderCacheClientManager {

  public IoTDBThriftSyncClientDataNodeManager(
      List<TEndPoint> endPoints,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache) {
    super(endPoints, useSSL, trustStorePath, trustStorePwd, useLeaderCache);
  }

  @Override
  protected PipeTransferDataNodeHandshakeV1Req buildHandshakeV1Req() throws IOException {
    return PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  @Override
  protected PipeTransferHandshakeV2Req buildHandshakeV2Req(Map<String, String> params)
      throws IOException {
    return PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params);
  }
}
