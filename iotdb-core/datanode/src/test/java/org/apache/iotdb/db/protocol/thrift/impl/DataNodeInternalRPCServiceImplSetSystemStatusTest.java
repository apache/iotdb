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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.DataNode.DataNodeContext;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class DataNodeInternalRPCServiceImplSetSystemStatusTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private NodeStatus originalStatus;
  private String originalStatusReason;

  @BeforeClass
  public static void setUpClass() {
    // The static initializer of DataNodeInternalRPCServiceImpl (Coordinator) requires it.
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @Before
  public void setUp() {
    originalStatus = commonConfig.getNodeStatus();
    originalStatusReason = commonConfig.getStatusReason();
    commonConfig.setNodeStatus(NodeStatus.Running);
  }

  @After
  public void tearDown() {
    commonConfig.setNodeStatus(originalStatus);
    commonConfig.setStatusReason(originalStatusReason);
  }

  @Test
  public void testSetSystemStatusReadOnlyCarriesManualReason() throws Exception {
    final DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(mock(DataNodeContext.class));

    // A ReadOnly requested through the RPC (i.e. by the ConfigNode on SET SYSTEM STATUS) is
    // recorded with the Manual reason.
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), service.setSystemStatus("ReadOnly").getCode());
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.MANUAL, commonConfig.getStatusReason());

    // Switching back to Running clears the reason.
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), service.setSystemStatus("Running").getCode());
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());
  }

  @Test
  public void testSetSystemStatusInvalidStatusReturnsError() throws Exception {
    final DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(mock(DataNodeContext.class));

    Assert.assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        service.setSystemStatus("NotAStatus").getCode());
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
  }
}
