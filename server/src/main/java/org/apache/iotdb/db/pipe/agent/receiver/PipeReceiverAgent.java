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

package org.apache.iotdb.db.pipe.agent.receiver;

import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorVersion;
import org.apache.iotdb.db.pipe.connector.v1.IoTDBThriftReceiverV1;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReceiverAgent.class);

  private final ThreadLocal<IoTDBThriftReceiver> receiverThreadLocal = new ThreadLocal<>();

  public TPipeTransferResp receive(
      TPipeTransferReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    final byte reqVersion = req.getVersion();
    if (reqVersion == IoTDBThriftConnectorVersion.VERSION_1.getVersion()) {
      return getReceiver(reqVersion).receive(req, partitionFetcher, schemaFetcher);
    } else {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_VERSION_ERROR,
              String.format("Unsupported pipe version %d", reqVersion)));
    }
  }

  private IoTDBThriftReceiver getReceiver(byte reqVersion) {
    if (receiverThreadLocal.get() == null) {
      return setAndGetReceiver(reqVersion);
    }

    final byte receiverThreadLocalVersion = receiverThreadLocal.get().getVersion().getVersion();
    if (receiverThreadLocalVersion != reqVersion) {
      LOGGER.warn(
          "The receiver version {} is different from the sender version {},"
              + " the receiver will be reset to the sender version.",
          receiverThreadLocalVersion,
          reqVersion);
      receiverThreadLocal.get().handleExit();
      receiverThreadLocal.remove();
      return setAndGetReceiver(reqVersion);
    }

    return receiverThreadLocal.get();
  }

  private IoTDBThriftReceiver setAndGetReceiver(byte reqVersion) {
    if (reqVersion == IoTDBThriftConnectorVersion.VERSION_1.getVersion()) {
      receiverThreadLocal.set(new IoTDBThriftReceiverV1());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported pipe version %d", reqVersion));
    }
    return receiverThreadLocal.get();
  }

  public void handleClientExit() {
    final IoTDBThriftReceiver receiver = receiverThreadLocal.get();
    if (receiver != null) {
      receiver.handleExit();
      receiverThreadLocal.remove();
    }
  }
}
