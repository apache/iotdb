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
package org.apache.iotdb.db.sync.transport.server;

import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SyncConnectionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SyncConnectionManager.class);

  // When the client abnormally exits, we can still know who to disconnect
  private final ThreadLocal<Long> currentConnectionId;
  // Record the remote message for every rpc connection
  private final Map<Long, TSyncIdentityInfo> connectionIdToIdentityInfoMap;

  // The sync connectionId is unique in one IoTDB instance.
  private final AtomicLong connectionIdGenerator;

  private SyncConnectionManager() {
    currentConnectionId = new ThreadLocal<>();
    connectionIdToIdentityInfoMap = new ConcurrentHashMap<>();
    connectionIdGenerator = new AtomicLong();
  }

  public static SyncConnectionManager getInstance() {
    return SyncConnectionManager.SyncConnectionManagerHolder.INSTANCE;
  }

  private static class SyncConnectionManagerHolder {
    private static final SyncConnectionManager INSTANCE = new SyncConnectionManager();

    private SyncConnectionManagerHolder() {}
  }

  /** Check if the connection is legally established by handshaking */
  public boolean checkConnection() {
    return currentConnectionId.get() != null;
  }

  /**
   * Get current TSyncIdentityInfo
   *
   * @return null if connection has been exited
   */
  public TSyncIdentityInfo getCurrentTSyncIdentityInfo() {
    Long id = currentConnectionId.get();
    if (id != null) {
      return connectionIdToIdentityInfoMap.get(id);
    } else {
      return null;
    }
  }

  public void createConnection(TSyncIdentityInfo identityInfo) {
    long connectionId = connectionIdGenerator.incrementAndGet();
    currentConnectionId.set(connectionId);
    connectionIdToIdentityInfoMap.put(connectionId, identityInfo);
  }

  public void exitConnection() {
    if (checkConnection()) {
      long id = currentConnectionId.get();
      connectionIdToIdentityInfoMap.remove(id);
      currentConnectionId.remove();
    }
  }

  public List<TSyncIdentityInfo> getAllTSyncIdentityInfos() {
    return new ArrayList<>(connectionIdToIdentityInfoMap.values());
  }
}
