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

package org.apache.iotdb.commons.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.QueryTimeoutException;

import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public class MPPDataExchangeClientManager
    extends ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

  public MPPDataExchangeClientManager() {
    super(new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory());
  }

  @Override
  public SyncDataNodeMPPDataExchangeServiceClient borrowClient(TEndPoint endpoint)
      throws ClientManagerException {
    return borrowClient(endpoint, Long.MAX_VALUE);
  }

  public SyncDataNodeMPPDataExchangeServiceClient borrowClient(TEndPoint endpoint, long deadlineMs)
      throws ClientManagerException {
    SyncDataNodeMPPDataExchangeServiceClient client = null;
    boolean ready = false;
    try {
      client = borrowClientWithTimeout(endpoint, remainingMillis(deadlineMs));
      client.prepare(deadlineMs);
      ready = true;
      return client;
    } catch (IOException | TTransportException | RuntimeException e) {
      throw new ClientManagerException(e);
    } finally {
      if (client != null && !ready) {
        try {
          client.invalidate();
        } finally {
          client.close();
        }
      }
    }
  }

  public static long remainingMillis(long deadlineMs) {
    if (deadlineMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long remaining = deadlineMs - System.currentTimeMillis();
    if (remaining <= 0) {
      throw new QueryTimeoutException();
    }
    return remaining;
  }
}
