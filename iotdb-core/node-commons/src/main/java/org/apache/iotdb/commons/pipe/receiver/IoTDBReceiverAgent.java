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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class IoTDBReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBReceiverAgent.class);

  protected ThreadLocal<IoTDBReceiver> receiverThreadLocal = new ThreadLocal<>();

  protected static final Map<Byte, Supplier<IoTDBReceiver>> RECEIVER_CONSTRUCTORS = new HashMap<>();

  protected abstract void initConstructors();

  protected IoTDBReceiverAgent() {
    initConstructors();
  }

  public final TPipeTransferResp receive(TPipeTransferReq req) {
    final byte reqVersion = req.getVersion();
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      return getReceiver(reqVersion).receive(req);
    } else {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_VERSION_ERROR,
              String.format("Unsupported pipe version %d", reqVersion)));
    }
  }

  protected final IoTDBReceiver getReceiver(byte reqVersion) {
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

  private IoTDBReceiver setAndGetReceiver(byte reqVersion) {
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      receiverThreadLocal.set(RECEIVER_CONSTRUCTORS.get(reqVersion).get());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported pipe version %d", reqVersion));
    }
    return receiverThreadLocal.get();
  }

  public final void handleClientExit() {
    final IoTDBReceiver receiver = receiverThreadLocal.get();
    if (receiver != null) {
      receiver.handleExit();
      receiverThreadLocal.remove();
    }
  }
}
