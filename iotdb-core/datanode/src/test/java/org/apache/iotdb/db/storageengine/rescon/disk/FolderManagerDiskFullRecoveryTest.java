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
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.utils.JVMCommonUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({CommonDescriptor.class, JVMCommonUtils.class})
public class FolderManagerDiskFullRecoveryTest {
  private CommonConfig commonConfig;

  @Before
  public void setUp() throws Exception {
    commonConfig = Whitebox.invokeConstructor(CommonConfig.class);
    PowerMockito.mockStatic(CommonDescriptor.class);
    CommonDescriptor descriptor = mock(CommonDescriptor.class);
    when(CommonDescriptor.getInstance()).thenReturn(descriptor);
    when(descriptor.getConfig()).thenReturn(commonConfig);
    PowerMockito.mockStatic(JVMCommonUtils.class);
  }

  @Test
  public void testFullSnapshotDirectoryDoesNotRecoverWhileFull() throws Exception {
    commonConfig.setNodeStatus(NodeStatus.Running);
    String snapshot = "bounded-data/snapshot";
    for (int i = 0; i < 3; i++) {
      // Constructor failure must be remembered even though no FolderManager is returned.
      Assert.assertThrows(
          DiskSpaceInsufficientException.class,
          () ->
              new FolderManager(
                  Collections.singletonList(snapshot), DirectoryStrategyType.SEQUENCE_STRATEGY));
      commonConfig.tryRecoverFromDiskFull();
      Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
      Assert.assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());
    }

    when(JVMCommonUtils.hasSpace(snapshot)).thenReturn(true);
    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());

    // A later business operation must still trigger protection if the directory becomes full again.
    when(JVMCommonUtils.hasSpace(snapshot)).thenReturn(false);
    Assert.assertThrows(
        DiskSpaceInsufficientException.class,
        () ->
            new FolderManager(
                Collections.singletonList(snapshot), DirectoryStrategyType.SEQUENCE_STRATEGY));
    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());
  }

  @Test
  public void testAllFailedGroupsMustRecoverButOneCandidatePerGroupIsEnough() throws Exception {
    for (String[] group : new String[][] {{"tier0/sequence", "tier0-spare/sequence"}, {"wal"}}) {
      Assert.assertThrows(
          DiskSpaceInsufficientException.class,
          () -> new FolderManager(Arrays.asList(group), DirectoryStrategyType.SEQUENCE_STRATEGY));
    }
    when(JVMCommonUtils.hasSpace("tier0-spare/sequence")).thenReturn(true);
    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());

    when(JVMCommonUtils.hasSpace("wal")).thenReturn(true);
    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());
  }

  @Test
  public void testRuntimeFailureCannotRecoverThroughAnAbnormalFolder() throws Exception {
    String full = "sequence/full";
    String abnormal = "sequence/abnormal";
    when(JVMCommonUtils.hasSpace(full)).thenReturn(true);
    when(JVMCommonUtils.hasSpace(abnormal)).thenReturn(true);
    FolderManager manager =
        new FolderManager(Arrays.asList(full, abnormal), DirectoryStrategyType.SEQUENCE_STRATEGY);
    manager.updateFolderState(abnormal, FolderManager.FolderState.ABNORMAL);
    when(JVMCommonUtils.hasSpace(full)).thenReturn(false);
    Assert.assertThrows(DiskSpaceInsufficientException.class, manager::getNextFolder);

    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());

    when(JVMCommonUtils.hasSpace(full)).thenReturn(true);
    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());
  }

  @Test
  public void testOptOutDirectoryFailureDoesNotBlockRecovery() throws Exception {
    commonConfig.setNodeStatus(NodeStatus.Running);
    Assert.assertThrows(
        DiskSpaceInsufficientException.class,
        () ->
            new FolderManager(
                Collections.singletonList("load-staging"),
                DirectoryStrategyType.SEQUENCE_STRATEGY,
                false));
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());

    commonConfig.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    commonConfig.tryRecoverFromDiskFull();
    Assert.assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    Assert.assertNull(commonConfig.getStatusReason());
  }
}
