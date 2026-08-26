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

package org.apache.iotdb.db.storageengine.rescon.disk;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class FolderManagerTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private NodeStatus originalStatus;
  private String originalStatusReason;

  @Before
  public void setUp() {
    originalStatus = commonConfig.getNodeStatus();
    originalStatusReason = commonConfig.getStatusReason();
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setStatusReason(null);
  }

  @After
  public void tearDown() {
    commonConfig.setNodeStatus(originalStatus);
    commonConfig.setStatusReason(originalStatusReason);
  }

  @Test
  public void testSkipNodeStatusChangeWhenDiskFull() {
    try {
      new FolderManager(Collections.emptyList(), DirectoryStrategyType.SEQUENCE_STRATEGY, false);
      Assert.fail("Expected DiskSpaceInsufficientException");
    } catch (DiskSpaceInsufficientException e) {
      Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
      Assert.assertNull(commonConfig.getStatusReason());
    }
  }

  @Test
  public void testChangeNodeStatusWhenDiskFullByDefault() {
    try {
      new FolderManager(Collections.emptyList(), DirectoryStrategyType.SEQUENCE_STRATEGY);
      Assert.fail("Expected DiskSpaceInsufficientException");
    } catch (DiskSpaceInsufficientException e) {
      Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
      Assert.assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());
    }
  }
}
