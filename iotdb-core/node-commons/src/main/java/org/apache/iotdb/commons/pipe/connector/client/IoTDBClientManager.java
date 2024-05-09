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

public abstract class IoTDBClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClientManager.class);

  protected final List<TEndPoint> endPointList;
  protected long currentClientIndex = 0;

  protected final boolean useLeaderCache;

  // This flag indicates whether the receiver supports mods transferring if
  // it is a DataNode receiver. The flag is useless for configNode receiver.
  protected boolean supportModsIfIsDataNodeReceiver = true;

  private static final int MAX_CONNECTION_TIMEOUT_MS = 24 * 60 * 60 * 1000; // 1 day
  protected int connectionTimeout = PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs();

  protected IoTDBClientManager(List<TEndPoint> endPointList, boolean useLeaderCache) {
    this.endPointList = endPointList;
    this.useLeaderCache = useLeaderCache;
  }

  public boolean supportModsIfIsDataNodeReceiver() {
    return supportModsIfIsDataNodeReceiver;
  }

  public void adjustTimeoutIfNecessary(Throwable e) {
    do {
      if (e instanceof SocketTimeoutException) {
        int newConnectionTimeout;
        try {
          newConnectionTimeout =
              Math.min(Math.toIntExact(connectionTimeout * 2L), MAX_CONNECTION_TIMEOUT_MS);
        } catch (ArithmeticException arithmeticException) {
          newConnectionTimeout = MAX_CONNECTION_TIMEOUT_MS;
        }

        if (newConnectionTimeout != connectionTimeout) {
          connectionTimeout = newConnectionTimeout;
          LOGGER.info(
              "Pipe connection timeout is adjusted to {} ms ({} mins)",
              connectionTimeout,
              connectionTimeout / 60000.0);
        }
        return;
      }
    } while ((e = e.getCause()) != null);
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }
}
