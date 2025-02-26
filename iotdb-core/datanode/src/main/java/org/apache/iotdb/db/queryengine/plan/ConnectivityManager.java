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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;

import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Connectivity Manager will report disconnection between internal datanode RPCs. Whenever received
 * TExceptions
 */
public class ConnectivityManager {

  private static final class ConnectivityManagerHolder {
    private ConnectivityManagerHolder() {}

    private static final ConnectivityManager INSTANCE = new ConnectivityManager();
  }

  public static ConnectivityManager getInstance() {
    return ConnectivityManagerHolder.INSTANCE;
  }

  private final ConcurrentMap<TEndPoint, EndpointState> nodeStatusMap = new ConcurrentHashMap<>();

  private static class EndpointState {
    private long lastTimestamp;
    private int failures;

    EndpointState() {
      lastTimestamp = 0L;
      failures = 0;
    }

    private synchronized void reportSuccess() {
      lastTimestamp = System.nanoTime();
      failures = 0;
    }

    private synchronized void reportFailure() {
      failures++;
    }

    private synchronized boolean isAlive() {
      // 10 seconds passed since the last success connection
      // at least 1 failure report
      return !(failures > 0 && System.nanoTime() - lastTimestamp > 10_000_000_000L);
    }
  }

  public void reportSuccess(TEndPoint endPoint) {
    nodeStatusMap.computeIfAbsent(endPoint, ep -> new EndpointState()).reportSuccess();
  }

  public void reportFailure(TEndPoint endPoint, TException exception) {
    nodeStatusMap.computeIfAbsent(endPoint, ep -> new EndpointState()).reportFailure();
  }

  public Map<TEndPoint, String> getLocalNodeStatus() {
    Map<TEndPoint, String> statusMap = new HashMap<>();
    Set<Map.Entry<TEndPoint, EndpointState>> entries = nodeStatusMap.entrySet();
    for (Map.Entry<TEndPoint, EndpointState> entry : entries) {
      if (entry.getValue().isAlive()) {
        statusMap.put(entry.getKey(), NodeStatus.Running.getStatus());
      } else {
        statusMap.put(entry.getKey(), NodeStatus.Unknown.getStatus());
      }
    }
    return statusMap;
  }
}
