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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataNodeShutdownHookTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private NodeStatus originalStatus;
  private String originalStatusReason;
  private boolean originalStopping;

  @Before
  public void setUp() {
    originalStatus = commonConfig.getNodeStatus();
    originalStatusReason = commonConfig.getStatusReason();
    originalStopping = commonConfig.isStopping();
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setStopping(false);
  }

  @After
  public void tearDown() {
    commonConfig.setNodeStatus(originalStatus);
    commonConfig.setStatusReason(originalStatusReason);
    commonConfig.setStopping(originalStopping);
  }

  @Test
  public void testMarkNodeStoppingSetsReadOnlyWithStoppingReason() {
    // The real shutdown hook performs this transition in the middle of its run() sequence; the
    // extracted method is invoked directly here so the transient ReadOnly(Stopping) state is
    // asserted deterministically instead of racing the shutdown window.
    DataNodeShutdownHook.markNodeStopping();

    Assert.assertTrue(commonConfig.isStopping());
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.STOPPING, commonConfig.getStatusReason());
  }
}
