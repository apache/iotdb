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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.receiver.thrift.IoTDBThriftReceiver;
import org.apache.iotdb.db.pipe.receiver.thrift.IoTDBThriftReceiverV1;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class PipeReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReceiverAgent.class);

  private final ThreadLocal<IoTDBThriftReceiver> receiverThreadLocal = new ThreadLocal<>();

  public TPipeTransferResp receive(
      TPipeTransferReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    final byte reqVersion = req.getVersion();
    if (reqVersion == IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion()) {
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
    if (reqVersion == IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion()) {
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

  public void cleanPipeReceiverDir() {
    final File receiverFileDir =
        new File(IoTDBDescriptor.getInstance().getConfig().getPipeReceiverFileDir());

    try {
      FileUtils.deleteDirectory(receiverFileDir);
      LOGGER.info("Clean pipe receiver dir {} successfully.", receiverFileDir);
    } catch (Exception e) {
      LOGGER.warn("Clean pipe receiver dir {} failed.", receiverFileDir, e);
    }

    try {
      FileUtils.forceMkdir(receiverFileDir);
      LOGGER.info("Create pipe receiver dir {} successfully.", receiverFileDir);
    } catch (IOException e) {
      LOGGER.warn("Create pipe receiver dir {} failed.", receiverFileDir, e);
    }
  }
}
