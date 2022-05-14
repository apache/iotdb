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
package org.apache.iotdb.confignode.manager.load.heartbeat;

import java.util.HashMap;
import java.util.Map;

/** HeartbeatCache caches and maintains all the heartbeat data */
public class HeartbeatCache implements IHeartbeatStatistic {

  private boolean containsCache = false;

  // Map<DataNodeId, HeartbeatWindow>
  private final Map<Integer, HeartbeatWindow> windowMap;

  public HeartbeatCache() {
    this.windowMap = new HashMap<>();
  }

  @Override
  public void cacheHeartBeat(int dataNodeId, HeartbeatPackage newHeartbeat) {
    containsCache = true;
    windowMap
        .computeIfAbsent(dataNodeId, window -> new HeartbeatWindow())
        .addHeartbeat(newHeartbeat);
  }

  @Override
  public void discardAllCache() {
    if (containsCache) {
      containsCache = false;
      windowMap.clear();
    }
  }
}
