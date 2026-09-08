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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TLoadSample;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.DataNode.DataNodeContext;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataNodeInternalRPCServiceImplDiskTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final IoTDBConfig dataNodeConfig = IoTDBDescriptor.getInstance().getConfig();
  private NodeStatus originalStatus;
  private String originalStatusReason;
  private double originalDiskSpaceWarningThreshold;
  private int originalDataNodeId;

  @Before
  public void setUp() {
    originalStatus = commonConfig.getNodeStatus();
    originalStatusReason = commonConfig.getStatusReason();
    originalDiskSpaceWarningThreshold = commonConfig.getDiskSpaceWarningThreshold();
    originalDataNodeId = dataNodeConfig.getDataNodeId();

    dataNodeConfig.setDataNodeId(0);
    // Go through Running first so that a ReadOnly with a higher-priority reason left behind by
    // another test can always be overridden.
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setDiskSpaceWarningThreshold(0.05);
    commonConfig.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
  }

  @After
  public void tearDown() {
    commonConfig.setNodeStatus(originalStatus);
    commonConfig.setStatusReason(originalStatusReason);
    commonConfig.setDiskSpaceWarningThreshold(originalDiskSpaceWarningThreshold);
    dataNodeConfig.setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testRunningRecoveryUsesAggregateDiskRatio() {
    SystemMetrics systemMetrics = mock(SystemMetrics.class);
    // The aggregate free ratio is 52%, so the node can recover to Running.
    when(systemMetrics.getSystemDiskAvailableSpace()).thenReturn(104L);
    when(systemMetrics.getSystemDiskTotalSpace()).thenReturn(200L);

    DataNodeContext dataNodeContext = mock(DataNodeContext.class);
    DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(dataNodeContext, systemMetrics);
    TLoadSample loadSample = new TLoadSample();

    service.sampleDiskLoad(loadSample);

    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());
    Assert.assertEquals(104.0, loadSample.getFreeDiskSpace(), 0.0);
    Assert.assertEquals(0.48, loadSample.getDiskUsageRate(), 1e-10);
  }

  @Test
  public void testStorageEngineDiskAggregateStillEntersReadOnly() {
    SystemMetrics systemMetrics = mock(SystemMetrics.class);
    when(systemMetrics.getSystemDiskAvailableSpace()).thenReturn(4L);
    when(systemMetrics.getSystemDiskTotalSpace()).thenReturn(100L);

    commonConfig.setNodeStatus(NodeStatus.Running);
    DataNodeContext dataNodeContext = mock(DataNodeContext.class);
    DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(dataNodeContext, systemMetrics);

    service.sampleDiskLoad(new TLoadSample());

    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());
  }

  @Test
  public void testRecoveryRequiresExactReadOnlyWithDiskFullReason() {
    SystemMetrics systemMetrics = mock(SystemMetrics.class);
    // The aggregate free ratio is 52%, so a ReadOnly + DiskFull state would recover.
    when(systemMetrics.getSystemDiskAvailableSpace()).thenReturn(104L);
    when(systemMetrics.getSystemDiskTotalSpace()).thenReturn(200L);

    DataNodeContext dataNodeContext = mock(DataNodeContext.class);
    DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(dataNodeContext, systemMetrics);

    // A manually set ReadOnly must not be auto-recovered by a disk ratio that recovered.
    commonConfig.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    service.sampleDiskLoad(new TLoadSample());
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.MANUAL, commonConfig.getStatusReason());

    // Neither can a ReadOnly entered through an unrecoverable error.
    // (Manual outranks UnrecoverableError, so go through Running to enter it.)
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setNodeStatusWithReason(
        NodeStatus.ReadOnly, NodeStatus.UNRECOVERABLE_ERROR + ", 2026-09-02 10:00:00.000, broken");
    service.sampleDiskLoad(new TLoadSample());
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertTrue(commonConfig.getStatusReason().startsWith(NodeStatus.UNRECOVERABLE_ERROR));
  }
}
