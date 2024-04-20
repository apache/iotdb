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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RepairLoggerTest extends AbstractRepairDataTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testSimpleReadWriteLogFile() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    TsFileResource resource2 = createEmptyFileAndResource(true);
    resource2.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    RepairTimePartition mockRepairTimePartition = Mockito.mock(RepairTimePartition.class);
    Mockito.when(mockRepairTimePartition.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockRepairTimePartition.getDataRegionId()).thenReturn("0");
    Mockito.when(mockRepairTimePartition.getTimePartitionId()).thenReturn(0L);
    Mockito.when(mockRepairTimePartition.getAllFileSnapshot())
        .thenReturn(Arrays.asList(resource1, resource2));
    File tempDir = getEmptyRepairDataLogDir();
    File logFile = null;
    try (RepairLogger logger = new RepairLogger(tempDir, false)) {
      logFile = logger.getLogFile();
      logger.recordRepairTaskStartTimeIfLogFileEmpty(System.currentTimeMillis());
      logger.recordRepairedTimePartition(mockRepairTimePartition);
    }
    RepairTaskRecoverLogParser logParser = new RepairTaskRecoverLogParser(logFile);
    logParser.parse();
    Map<RepairTimePartition, Set<String>> repairedTimePartitionsWithCannotRepairFiles =
        logParser.getRepairedTimePartitionsWithCannotRepairFiles();
    Assert.assertEquals(1, repairedTimePartitionsWithCannotRepairFiles.size());
    for (Map.Entry<RepairTimePartition, Set<String>> entry :
        repairedTimePartitionsWithCannotRepairFiles.entrySet()) {
      Assert.assertEquals(
          mockRepairTimePartition.getDatabaseName(), entry.getKey().getDatabaseName());
      Assert.assertEquals(
          mockRepairTimePartition.getDataRegionId(), entry.getKey().getDataRegionId());
      Assert.assertEquals(
          mockRepairTimePartition.getTimePartitionId(), entry.getKey().getTimePartitionId());
      Assert.assertTrue(entry.getValue().contains(resource2.getTsFile().getName()));
      break;
    }
  }

  @Test
  public void testReadManyTimePartitionLogFile() throws IOException {
    RepairTimePartition mockRepairTimePartition = Mockito.mock(RepairTimePartition.class);
    Mockito.when(mockRepairTimePartition.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockRepairTimePartition.getDataRegionId()).thenReturn("0");
    Mockito.when(mockRepairTimePartition.getTimePartitionId()).thenReturn(0L, 1L, 2L);
    Mockito.when(mockRepairTimePartition.getAllFileSnapshot()).thenReturn(Collections.emptyList());
    File tempDir = getEmptyRepairDataLogDir();
    File logFile = null;
    try (RepairLogger logger = new RepairLogger(tempDir, false)) {
      logFile = logger.getLogFile();
      logger.recordRepairTaskStartTimeIfLogFileEmpty(System.currentTimeMillis());
      for (int i = 0; i < 3; i++) {
        logger.markStartOfRepairedTimePartition(mockRepairTimePartition);
        logger.recordCannotRepairFiles(mockRepairTimePartition);
        logger.markEndOfRepairedTimePartition(mockRepairTimePartition);
      }
    }
    RepairTaskRecoverLogParser logParser = new RepairTaskRecoverLogParser(logFile);
    logParser.parse();
    Map<RepairTimePartition, Set<String>> repairedTimePartitionsWithCannotRepairFiles =
        logParser.getRepairedTimePartitionsWithCannotRepairFiles();
    Assert.assertEquals(3, repairedTimePartitionsWithCannotRepairFiles.size());
  }

  @Test
  public void testReadIncompleteLogFile() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    TsFileResource resource2 = createEmptyFileAndResource(true);
    resource2.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    RepairTimePartition mockRepairTimePartition = Mockito.mock(RepairTimePartition.class);
    Mockito.when(mockRepairTimePartition.getDatabaseName()).thenReturn("root.testsg");
    Mockito.when(mockRepairTimePartition.getDataRegionId()).thenReturn("0");
    Mockito.when(mockRepairTimePartition.getTimePartitionId()).thenReturn(0L);
    Mockito.when(mockRepairTimePartition.getAllFileSnapshot())
        .thenReturn(Arrays.asList(resource1, resource2));
    File tempDir = getEmptyRepairDataLogDir();
    File logFile = null;
    try (RepairLogger logger = new RepairLogger(tempDir, false)) {
      logger.recordRepairTaskStartTimeIfLogFileEmpty(System.currentTimeMillis());
      logFile = logger.getLogFile();
      logger.markStartOfRepairedTimePartition(mockRepairTimePartition);
      logger.recordCannotRepairFiles(mockRepairTimePartition);
    }
    RepairTaskRecoverLogParser logParser = new RepairTaskRecoverLogParser(logFile);
    logParser.parse();
    Map<RepairTimePartition, Set<String>> repairedTimePartitionsWithCannotRepairFiles =
        logParser.getRepairedTimePartitionsWithCannotRepairFiles();
    Assert.assertEquals(1, repairedTimePartitionsWithCannotRepairFiles.size());
    for (Map.Entry<RepairTimePartition, Set<String>> entry :
        repairedTimePartitionsWithCannotRepairFiles.entrySet()) {
      Assert.assertEquals(
          mockRepairTimePartition.getDatabaseName(), entry.getKey().getDatabaseName());
      Assert.assertEquals(
          mockRepairTimePartition.getDataRegionId(), entry.getKey().getDataRegionId());
      Assert.assertEquals(
          mockRepairTimePartition.getTimePartitionId(), entry.getKey().getTimePartitionId());
      Assert.assertTrue(entry.getValue().contains(resource2.getTsFile().getName()));
      break;
    }
  }
}
