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
 *
 */

package org.apache.iotdb.db.engine.archiving;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.PauseArchivingPlan;
import org.apache.iotdb.db.qp.physical.sys.SetArchivingPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowArchivingPlan;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ArchivingTest {
  private final String sg1 = "root.ARCHIVING_SG1";
  private final String sg2 = "root.ARCHIVING_SG2";
  private final long ttl = 12345;
  private long startTime; // 2023-01-01
  private VirtualStorageGroupProcessor virtualStorageGroupProcessor;
  private final String s1 = "s1";
  private final String g1s1 = sg1 + IoTDBConstant.PATH_SEPARATOR + s1;
  private long prevPartitionInterval;
  private File targetDir;

  @Before
  public void setUp()
      throws MetadataException, StorageGroupProcessorException, LogicalOperatorException {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.envSetUp();
    createSchemas();
    targetDir = new File("separated_test");
    targetDir.mkdirs();

    startTime = DateTimeUtils.convertDatetimeStrToLong("2023-01-01", ZoneId.systemDefault());
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
    File[] movedFiles = targetDir.listFiles();
    if (movedFiles != null) {
      for (File file : movedFiles) {
        file.delete();
      }
    }
    targetDir.delete();
  }

  private void createSchemas() throws MetadataException, StorageGroupProcessorException {
    virtualStorageGroupProcessor =
        new VirtualStorageGroupProcessor(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            sg1,
            new DirectFlushPolicy(),
            sg1);
    IoTDB.metaManager.createTimeseries(
        new PartialPath(g1s1),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        Collections.emptyMap());
  }

  private void prepareData()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
    InsertRowPlan plan = new InsertRowPlan();
    plan.setDevicePath(new PartialPath(sg1));
    plan.setTime(System.currentTimeMillis());
    plan.setMeasurements(new String[] {"s1"});
    plan.setDataTypes(new TSDataType[] {TSDataType.INT64});
    plan.setValues(new Object[] {1L});
    plan.setMeasurementMNodes(
        new IMeasurementMNode[] {
          MeasurementMNode.getMeasurementMNode(
              null, "s1", new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null)
        });
    plan.transferType();

    long initTime = System.currentTimeMillis();
    // sequence data
    for (int i = 1000; i < 2000; i++) {
      plan.setTime(initTime - 2000 + i);
      virtualStorageGroupProcessor.insert(plan);
      if ((i + 1) % 300 == 0) {
        virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
    // unsequence data
    for (int i = 0; i < 1000; i++) {
      plan.setTime(initTime - 2000 + i);
      virtualStorageGroupProcessor.insert(plan);
      if ((i + 1) % 300 == 0) {
        virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  @Test
  public void testArchiving()
      throws StorageEngineException, WriteProcessException, QueryProcessException,
          IllegalPathException, IOException {
    prepareData();

    virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    // files before ttl
    File seqDir = new File(DirectoryManager.getInstance().getNextFolderForSequenceFile(), sg1);
    File unseqDir = new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(), sg1);

    List<File> seqFiles = new ArrayList<>();
    for (File directory : seqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isDirectory()) {
            for (File tsfile : file.listFiles()) {
              if (tsfile.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
                seqFiles.add(file);
              }
            }
          }
        }
      }
    }

    List<File> unseqFiles = new ArrayList<>();
    for (File directory : unseqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isDirectory()) {
            for (File tsfile : file.listFiles()) {
              if (tsfile.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
                unseqFiles.add(file);
              }
            }
          }
        }
      }
    }

    assertEquals(4, seqFiles.size());
    assertEquals(4, unseqFiles.size());

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // create a new ArchivingTask with specified params
    ArchivingTask task = new ArchivingTask(0, new PartialPath(sg1), targetDir, 500, 0);
    task.setStatus(ArchivingTask.ArchivingTaskStatus.RUNNING);
    task.startTask();
    virtualStorageGroupProcessor.checkArchivingTask(task);
    task.close();

    // files after archiving
    seqFiles = new ArrayList<>();
    for (File directory : seqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isDirectory()) {
            for (File tsfile : file.listFiles()) {
              if (tsfile.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
                seqFiles.add(file);
              }
            }
          }
        }
      }
    }

    unseqFiles = new ArrayList<>();
    for (File directory : unseqDir.listFiles()) {
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isDirectory()) {
            for (File tsfile : file.listFiles()) {
              if (tsfile.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
                unseqFiles.add(file);
              }
            }
          }
        }
      }
    }

    assertTrue(seqFiles.size() <= 2);
    assertEquals(0, unseqFiles.size());

    List<File> targetFiles = new ArrayList<>();
    for (File tsfile : targetDir.listFiles()) {
      if (tsfile.getPath().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
        targetFiles.add(tsfile);
      }
    }

    assertEquals(8, targetFiles.size() + seqFiles.size() + unseqFiles.size());
  }

  @Test
  public void testParseSetArchiving() throws QueryProcessException {
    Planner planner = new Planner();
    SetArchivingPlan plan =
        (SetArchivingPlan)
            planner.parseSQLToPhysicalPlan(
                String.format(
                    "SET ARCHIVING TO %s 2023-01-01 10000 '%s'", sg1, targetDir.getPath()));
    assertEquals(sg1, plan.getStorageGroup().getFullPath());
    assertEquals(10000, plan.getTTL());
    assertEquals(startTime, plan.getStartTime());
    assertEquals(targetDir.getPath(), plan.getTargetDir().getPath());

    plan =
        (SetArchivingPlan)
            planner.parseSQLToPhysicalPlan(
                String.format(
                    "SET ARCHIVING TO start_time=2023-01-01 storage_group=%s ttl=10000 target_dir='%s'",
                    sg1, targetDir.getPath()));
    assertEquals(sg1, plan.getStorageGroup().getFullPath());
    assertEquals(10000, plan.getTTL());
    assertEquals(startTime, plan.getStartTime());
    assertEquals(targetDir.getPath(), plan.getTargetDir().getPath());

    plan = (SetArchivingPlan) planner.parseSQLToPhysicalPlan("CANCEL ARCHIVING ON " + sg2);
    assertEquals(sg2, plan.getStorageGroup().getFullPath());
    assertEquals(Long.MAX_VALUE, plan.getTTL());
    assertEquals(Long.MAX_VALUE, plan.getStartTime());

    plan = (SetArchivingPlan) planner.parseSQLToPhysicalPlan("CANCEL ARCHIVING 99");
    assertEquals(99, plan.getTaskId());
    assertEquals(Long.MAX_VALUE, plan.getTTL());
    assertEquals(Long.MAX_VALUE, plan.getStartTime());
  }

  @Test
  public void testParsePauseArchiving() throws QueryProcessException {
    Planner planner = new Planner();
    PauseArchivingPlan plan =
        (PauseArchivingPlan) planner.parseSQLToPhysicalPlan("PAUSE ARCHIVING ON " + sg2);
    assertEquals(sg2, plan.getStorageGroup().getFullPath());
    assertTrue(plan.isPause());

    plan = (PauseArchivingPlan) planner.parseSQLToPhysicalPlan("PAUSE ARCHIVING 10");
    assertEquals(10, plan.getTaskId());
    assertTrue(plan.isPause());

    plan = (PauseArchivingPlan) planner.parseSQLToPhysicalPlan("RESUME ARCHIVING ON " + sg1);
    assertEquals(sg1, plan.getStorageGroup().getFullPath());
    assertFalse(plan.isPause());

    plan = (PauseArchivingPlan) planner.parseSQLToPhysicalPlan("RESUME ARCHIVING 16");
    assertEquals(16, plan.getTaskId());
    assertFalse(plan.isPause());
  }

  @Test
  public void testParseShowArchiving() throws QueryProcessException {
    Planner planner = new Planner();
    ShowArchivingPlan plan =
        (ShowArchivingPlan) planner.parseSQLToPhysicalPlan("SHOW ALL ARCHIVING");
    assertTrue(plan.getStorageGroups().isEmpty());

    plan = (ShowArchivingPlan) planner.parseSQLToPhysicalPlan("SHOW ARCHIVING ON " + sg1);
    assertEquals(sg1, plan.getStorageGroups().get(0).getFullPath());
  }

  @Test
  public void testShowArchiving()
      throws IOException, QueryProcessException, QueryFilterOptimizationException,
          StorageEngineException, MetadataException, InterruptedException {
    ArchivingManager archivingManager = ArchivingManager.getInstance();
    archivingManager.setArchiving(new PartialPath(sg1), targetDir, ttl, startTime);

    ShowArchivingPlan plan = new ShowArchivingPlan(Collections.emptyList(), true);
    PlanExecutor executor = new PlanExecutor();
    QueryDataSet queryDataSet = executor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      String sg = rowRecord.getFields().get(2).getStringValue();
      if (sg.equals(sg1)) {
        ZonedDateTime startDate = DateTimeUtils.convertMillsecondToZonedDateTime(startTime);
        assertEquals(
            DateTimeUtils.ISO_OFFSET_DATE_TIME_WITH_MS.format(startDate),
            rowRecord.getFields().get(4).getStringValue());
        assertEquals(ttl, rowRecord.getFields().get(5).getLongV());
        assertEquals(targetDir.getPath(), rowRecord.getFields().get(6).getStringValue());
      } else {
        fail();
      }
    }
  }

  @Test
  public void testArchivingCleanFile()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
    prepareData();
    virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    assertEquals(4, virtualStorageGroupProcessor.getSequenceFileTreeSet().size());
    assertEquals(4, virtualStorageGroupProcessor.getUnSequenceFileList().size());

    ArchivingTask task = new ArchivingTask(0, new PartialPath(sg1), targetDir, 0, 0);
    task.setStatus(ArchivingTask.ArchivingTaskStatus.RUNNING);
    virtualStorageGroupProcessor.checkArchivingTask(task);

    assertEquals(0, virtualStorageGroupProcessor.getSequenceFileTreeSet().size());
    assertEquals(0, virtualStorageGroupProcessor.getUnSequenceFileList().size());
    task.close();
  }
}
