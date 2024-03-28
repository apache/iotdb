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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlowMonitorManager {

  private static final Logger logger = LoggerFactory.getLogger(FlowMonitorManager.class);
  public static final FlowMonitorManager INSTANCE = new FlowMonitorManager();

  private Map<TEndPoint, FlowMonitor> monitorMap = new ConcurrentHashMap<>();
  private RaftConfig config;

  private FlowMonitorManager() {}

  public void setConfig(RaftConfig config) {
    this.config = config;
  }

  public void close() {
    for (FlowMonitor flowMonitor : monitorMap.values()) {
      flowMonitor.close();
    }
    monitorMap.clear();
  }

  public void report(TEndPoint endPoint, long val) {
    FlowMonitor flowMonitor =
        monitorMap.computeIfAbsent(
            endPoint,
            p -> {
              try {
                return new FlowMonitor(p, config);
              } catch (IOException e) {
                logger.warn("Cannot register flow monitor for {}", endPoint, e);
                return null;
              }
            });
    if (flowMonitor != null) {
      flowMonitor.report(val);
    } else {
      logger.warn("Flow monitor {} is not registered", endPoint);
    }
  }

  public double averageFlow(TEndPoint endPoint, int windowsToUse) {
    FlowMonitor flowMonitor = monitorMap.get(endPoint);
    return flowMonitor != null ? flowMonitor.averageFlow(windowsToUse) : 0.0;
  }

  public List<FlowWindow> getLatestWindows(TEndPoint endPoint, int windowNum) {
    FlowMonitor flowMonitor = monitorMap.get(endPoint);
    return flowMonitor != null ? flowMonitor.getLatestWindows(windowNum) : Collections.emptyList();
  }

  public Map<TEndPoint, FlowMonitor> getMonitorMap() {
    return monitorMap;
  }
}
