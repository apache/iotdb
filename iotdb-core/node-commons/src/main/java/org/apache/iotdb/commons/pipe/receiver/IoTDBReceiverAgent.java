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

package org.apache.iotdb.commons.pipe.receiver;

import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class IoTDBReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBReceiverAgent.class);

  protected static final Map<Byte, Supplier<IoTDBReceiver>> RECEIVER_CONSTRUCTORS = new HashMap<>();

  protected abstract void initConstructors();

  protected IoTDBReceiverAgent() {
    initConstructors();
  }

  public final TPipeTransferResp receive(final TPipeTransferReq req) {
    return receive(null, req);
  }

  public final TPipeTransferResp receive(final String key, final TPipeTransferReq req) {
    final byte reqVersion = req.getVersion();
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      return getReceiver(key, reqVersion).receive(req);
    } else {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_VERSION_ERROR,
              String.format("Unsupported pipe version %d", reqVersion)));
    }
  }

  protected final IoTDBReceiver getReceiver(final String key, final byte reqVersion) {
    if (getReceiverWithSpecifiedClient(key) == null) {
      return setAndGetReceiver(key, reqVersion);
    }

    final byte receiverThreadLocalVersion =
        getReceiverWithSpecifiedClient(key).getVersion().getVersion();
    if (receiverThreadLocalVersion != reqVersion) {
      LOGGER.warn(
          "The receiver version {} is different from the sender version {},"
              + " the receiver will be reset to the sender version.",
          receiverThreadLocalVersion,
          reqVersion);
      getReceiverWithSpecifiedClient(key).handleExit();
      removeReceiverWithSpecifiedClient(key);
      return setAndGetReceiver(key, reqVersion);
    }

    return getReceiverWithSpecifiedClient(key);
  }

  private IoTDBReceiver setAndGetReceiver(final String key, final byte reqVersion) {
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      setReceiverWithSpecifiedClient(key, RECEIVER_CONSTRUCTORS.get(reqVersion).get());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported pipe version %d", reqVersion));
    }
    return getReceiverWithSpecifiedClient(key);
  }

  protected abstract IoTDBReceiver getReceiverWithSpecifiedClient(final String key);

  protected abstract void setReceiverWithSpecifiedClient(
      final String key, final IoTDBReceiver receiver);

  protected abstract void removeReceiverWithSpecifiedClient(final String key);

  public final void handleClientExit() {
    handleClientExit(null);
  }

  public final void handleClientExit(final String key) {
    final IoTDBReceiver receiver = getReceiverWithSpecifiedClient(key);
    if (receiver != null) {
      receiver.handleExit();
      removeReceiverWithSpecifiedClient(key);
    }
  }

  public static void cleanPipeReceiverDir(final File receiverFileDir) {
    try {
      RetryUtils.retryOnException(
          () -> {
            FileUtils.deleteDirectory(receiverFileDir);
            return null;
          });
      LOGGER.info("Clean pipe receiver dir {} successfully.", receiverFileDir);
    } catch (final Exception e) {
      LOGGER.warn("Clean pipe receiver dir {} failed.", receiverFileDir, e);
    }

    try {
      FileUtils.forceMkdir(receiverFileDir);
      LOGGER.info("Create pipe receiver dir {} successfully.", receiverFileDir);
    } catch (final IOException e) {
      LOGGER.warn("Create pipe receiver dir {} failed.", receiverFileDir, e);
    }
  }
}
