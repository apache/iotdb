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

package org.apache.iotdb.confignode.manager.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.service.ConfigNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IoTDBConfigNodeSyncClientManager extends IoTDBSyncClientManager {

  public IoTDBConfigNodeSyncClientManager(
      List<TEndPoint> endPoints,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      /* The following parameters are used locally. */
      String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      String username,
      String password,
      boolean shouldReceiverConvertOnTypeMismatch,
      String loadTsFileStrategy) {
    super(
        endPoints,
        useSSL,
        trustStorePath,
        trustStorePwd,
        false,
        loadBalanceStrategy,
        username,
        password,
        shouldReceiverConvertOnTypeMismatch,
        loadTsFileStrategy);
  }

  @Override
  protected PipeTransferConfigNodeHandshakeV1Req buildHandshakeV1Req() throws IOException {
    return PipeTransferConfigNodeHandshakeV1Req.toTPipeTransferReq(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  @Override
  protected PipeTransferHandshakeV2Req buildHandshakeV2Req(Map<String, String> params)
      throws IOException {
    return PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferReq(params);
  }

  @Override
  protected String getClusterId() {
    return ConfigNode.getInstance().getConfigManager().getClusterManager().getClusterId();
  }
}
