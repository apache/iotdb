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

package org.apache.iotdb.commons.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IoTDBClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClientManager.class);

  protected final List<TEndPoint> endPointList;
  protected long currentClientIndex = 0;

  protected final boolean useLeaderCache;

  protected final String username;
  protected final String password;

  protected final boolean shouldReceiverConvertOnTypeMismatch;
  protected final String loadTsFileStrategy;

  // This flag indicates whether the receiver supports mods transferring if
  // it is a DataNode receiver. The flag is useless for configNode receiver.
  protected boolean supportModsIfIsDataNodeReceiver = true;

  private static final int MAX_CONNECTION_TIMEOUT_MS = 24 * 60 * 60 * 1000; // 1 day
  private static final int FIRST_ADJUSTMENT_TIMEOUT_MS = 6 * 60 * 60 * 1000; // 6 hours
  protected static final AtomicInteger CONNECTION_TIMEOUT_MS =
      new AtomicInteger(PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());

  protected IoTDBClientManager(
      final List<TEndPoint> endPointList,
      /* The following parameters are used locally. */
      final boolean useLeaderCache,
      /* The following parameters are used to handshake with the receiver. */
      final String username,
      final String password,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy) {
    this.endPointList = endPointList;

    this.useLeaderCache = useLeaderCache;

    this.username = username;
    this.password = password;
    this.shouldReceiverConvertOnTypeMismatch = shouldReceiverConvertOnTypeMismatch;
    this.loadTsFileStrategy = loadTsFileStrategy;
  }

  public boolean supportModsIfIsDataNodeReceiver() {
    return supportModsIfIsDataNodeReceiver;
  }

  public void adjustTimeoutIfNecessary(Throwable e) {
    do {
      if (e instanceof SocketTimeoutException || e instanceof TimeoutException) {
        int newConnectionTimeout;
        try {
          newConnectionTimeout =
              Math.min(
                  Math.max(
                      FIRST_ADJUSTMENT_TIMEOUT_MS,
                      Math.toIntExact(CONNECTION_TIMEOUT_MS.get() * 2L)),
                  MAX_CONNECTION_TIMEOUT_MS);
        } catch (ArithmeticException arithmeticException) {
          newConnectionTimeout = MAX_CONNECTION_TIMEOUT_MS;
        }

        if (newConnectionTimeout != CONNECTION_TIMEOUT_MS.get()) {
          CONNECTION_TIMEOUT_MS.set(newConnectionTimeout);
          LOGGER.info(
              "Pipe connection timeout is adjusted to {} ms ({} mins)",
              newConnectionTimeout,
              newConnectionTimeout / 60000.0);
        }
        return;
      }
    } while ((e = e.getCause()) != null);
  }

  public int getConnectionTimeout() {
    return CONNECTION_TIMEOUT_MS.get();
  }
}
