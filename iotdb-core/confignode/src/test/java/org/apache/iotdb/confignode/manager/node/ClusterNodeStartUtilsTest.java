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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ClusterNodeStartUtilsTest {

  @Test
  public void confirmNodeRestartShouldRejectAINodeEndpointChange() {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    Mockito.when(configManager.getNodeManager()).thenReturn(nodeManager);
    Mockito.when(nodeManager.getRegisteredAINodes())
        .thenReturn(Collections.singletonList(aiNodeConfiguration(3, "127.0.0.1", 10810)));

    TSStatus status =
        ClusterNodeStartUtils.confirmNodeRestart(
            NodeType.AINode,
            ConfigNodeDescriptor.getInstance().getConf().getClusterName(),
            null,
            3,
            new TAINodeLocation(3, new TEndPoint("127.0.0.1", 10811)),
            configManager);

    Assert.assertEquals(TSStatusCode.REJECT_NODE_START.getStatusCode(), status.getCode());
  }

  @Test
  public void confirmNodeRestartShouldAcceptSameAINodeEndpoint() {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    Mockito.when(configManager.getNodeManager()).thenReturn(nodeManager);
    Mockito.when(nodeManager.getRegisteredAINodes())
        .thenReturn(Collections.singletonList(aiNodeConfiguration(3, "127.0.0.1", 10810)));

    TSStatus status =
        ClusterNodeStartUtils.confirmNodeRestart(
            NodeType.AINode,
            ConfigNodeDescriptor.getInstance().getConf().getClusterName(),
            null,
            3,
            new TAINodeLocation(3, new TEndPoint("127.0.0.1", 10810)),
            configManager);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  private TAINodeConfiguration aiNodeConfiguration(int aiNodeId, String ip, int port) {
    return new TAINodeConfiguration(
        new TAINodeLocation(aiNodeId, new TEndPoint(ip, port)), new TNodeResource(1, 1024));
  }
}
