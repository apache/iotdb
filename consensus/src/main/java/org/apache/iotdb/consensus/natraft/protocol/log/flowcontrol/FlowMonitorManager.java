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

package org.apache.iotdb.consensus.natraft.protocol.log.flowcontrol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  public void register(TEndPoint node) {
    logger.info("Registering flow monitor {}", node);
    monitorMap.computeIfAbsent(
        node,
        n -> {
          try {
            return new FlowMonitor(n, config);
          } catch (IOException e) {
            logger.warn("Cannot register flow monitor for {}", node, e);
            return null;
          }
        });
  }

  public void report(TEndPoint node, long val) {
    FlowMonitor flowMonitor = monitorMap.get(node);
    if (flowMonitor != null) {
      flowMonitor.report(val);
    } else {
      logger.warn("Flow monitor {} is not registered", node);
    }
  }

  public double averageFlow(TEndPoint node, int windowsToUse) {
    return monitorMap.get(node).averageFlow(windowsToUse);
  }
}
