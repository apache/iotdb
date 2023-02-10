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

package org.apache.iotdb.cluster.expr.flowcontrol;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlowMonitorManager {

  private static final Logger logger = LoggerFactory.getLogger(FlowMonitorManager.class);
  public static final FlowMonitorManager INSTANCE = new FlowMonitorManager();

  private Map<Node, FlowMonitor> monitorMap = new ConcurrentHashMap<>();
  private ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();

  private FlowMonitorManager() {}

  public void close() {
    for (FlowMonitor flowMonitor : monitorMap.values()) {
      flowMonitor.close();
    }
    monitorMap.clear();
  }

  public void register(Node node) {
    logger.info("Registering flow monitor {}", node);
    monitorMap.computeIfAbsent(
        node,
        n -> {
          try {
            return new FlowMonitor(
                clusterConfig.getFlowMonitorMaxWindowSize(),
                clusterConfig.getFlowMonitorWindowInterval(),
                n);
          } catch (IOException e) {
            logger.warn("Cannot register flow monitor for {}", node, e);
            return null;
          }
        });
  }

  public void report(Node node, long val) {
    FlowMonitor flowMonitor = monitorMap.get(node);
    if (flowMonitor != null) {
      flowMonitor.report(val);
    } else {
      logger.warn("Flow monitor {} is not registered", node);
    }
  }

  public double averageFlow(Node node, int windowsToUse) {
    return monitorMap.get(node).averageFlow(windowsToUse);
  }
}
