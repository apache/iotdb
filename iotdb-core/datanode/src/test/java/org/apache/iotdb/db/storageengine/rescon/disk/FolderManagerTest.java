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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class FolderManagerTest {

  private FolderManager folderManager;
  private List<String> testFolders = Arrays.asList("/folder1", "/folder2", "/folder3");

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws DiskSpaceInsufficientException {
    folderManager = new FolderManager(testFolders, DirectoryStrategyType.SEQUENCE_STRATEGY);
  }

  @After
  public void tearDown() {
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.Running);
  }

  @Test
  public void testGetNextWithRetrySuccess() throws Exception {
    String expectedResult = "success";
    FolderManager.ThrowingFunction<String, String, Exception> function = folder -> expectedResult;
    String result = folderManager.getNextWithRetry(function);
    assertEquals(expectedResult, result);
  }

  @Test(expected = DiskSpaceInsufficientException.class)
  public void testGetNextWithRetryWithDiskFullException() throws Exception {
    FolderManager.ThrowingFunction<String, String, Exception> function =
        folder -> {
          throw new DiskSpaceInsufficientException(testFolders);
        };
    thrown.expect(DiskSpaceInsufficientException.class);
    folderManager.getNextWithRetry(function);
    assertEquals(NodeStatus.ReadOnly, CommonDescriptor.getInstance().getConfig().getNodeStatus());
    assertEquals(
        NodeStatus.DISK_FULL, CommonDescriptor.getInstance().getConfig().getStatusReason());
  }

  @Test
  public void testGetNextWithRetryWithRetryableException() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    String expectedResult = "success";

    FolderManager.ThrowingFunction<String, String, Exception> function =
        folder -> {
          if (counter.getAndIncrement() < 2) {
            throw new RuntimeException("mock exception");
          }
          return expectedResult;
        };
    String result = folderManager.getNextWithRetry(function);
    assertEquals(expectedResult, result);
    assertEquals(3, counter.get()); // one times failure, two times retry
  }

  @Test
  public void testGetNextWithRetryAllFoldersAbnormal() throws Exception {
    FolderManager.ThrowingFunction<String, String, Exception> function =
        folder -> {
          throw new RuntimeException("mock exception");
        };
    try {
      folderManager.getNextWithRetry(function);
      fail("Expected exception was not thrown");
    } catch (DiskSpaceInsufficientException e) {
      assertFalse(folderManager.hasHealthyFolder());
    }
  }
}
