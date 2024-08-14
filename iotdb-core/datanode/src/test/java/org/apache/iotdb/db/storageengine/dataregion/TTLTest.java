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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Thread.sleep;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TTLTest {

  private String sg1 = "root.TTL_SG1";
  private DataRegionId dataRegionId1 = new DataRegionId(1);
  private String sg2 = "root.TTL_SG2";
  private DataRegionId dataRegionId2 = new DataRegionId(1);
  private long ttl = 12345;
  private DataRegion dataRegion;
  private String s1 = "s1";
  private String g1s1 = sg1 + IoTDBConstant.PATH_SEPARATOR + s1;
  private long prevPartitionInterval;

  @Before
  public void setUp() throws MetadataException, DataRegionException {
    prevPartitionInterval = CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();
    CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(86400000);
    EnvironmentUtils.envSetUp();
    dataRegion =
        new DataRegion(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            String.valueOf(dataRegionId1.getId()),
            new DirectFlushPolicy(),
            sg1);
    //    createSchemas();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    dataRegion.abortCompaction();
    EnvironmentUtils.cleanEnv();
    CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(prevPartitionInterval);
    DataNodeTTLCache.getInstance().clearAllTTL();
  }

  @Test
  public void testTTLWrite()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(sg1),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            System.currentTimeMillis(),
            new Object[] {1L},
            false);
    node.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN)});

    // ok without ttl
    dataRegion.insert(node);
    DataNodeTTLCache.getInstance().setTTL(sg1, 1000);
    // with ttl
    node.setTime(System.currentTimeMillis() - 1001);
    boolean caught = false;
    try {
      dataRegion.insert(node);
    } catch (OutOfTTLException e) {
      caught = true;
    }
    assertTrue(caught);
    node.setTime(System.currentTimeMillis() - 900);
    dataRegion.insert(node);
  }

  private void prepareData() throws WriteProcessException, IllegalPathException {
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(sg1),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            System.currentTimeMillis(),
            new Object[] {1L},
            false);
    node.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN)});

    long initTime = System.currentTimeMillis();
    // sequence data
    for (int i = 1000; i < 2000; i++) {
      node.setTime(initTime - 2000 + i);
      dataRegion.insert(node);
      if ((i + 1) % 300 == 0) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      }
    }
    // unsequence data
    for (int i = 0; i < 1000; i++) {
      node.setTime(initTime - 2000 + i);
      dataRegion.insert(node);
      if ((i + 1) % 300 == 0) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  @Test
  public void testTTLRead()
      throws IOException, WriteProcessException, QueryProcessException, MetadataException {
    prepareData();

    // files before ttl
    QueryDataSource dataSource =
        dataRegion.query(
            Collections.singletonList(mockMeasurementPath()),
            IDeviceID.Factory.DEFAULT_FACTORY.create(sg1),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    List<TsFileResource> seqResource = dataSource.getSeqResources();
    List<TsFileResource> unseqResource = dataSource.getUnseqResources();
    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());

    DataNodeTTLCache.getInstance().setTTL(sg1, 500);

    // files after ttl
    dataSource =
        dataRegion.query(
            Collections.singletonList(mockMeasurementPath()),
            IDeviceID.Factory.DEFAULT_FACTORY.create(sg1),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();

    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());
    NonAlignedFullPath path = mockMeasurementPath();

    IDataBlockReader reader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(TEST_QUERY_JOB_ID),
            seqResource,
            unseqResource,
            true);

    int cnt = 0;
    while (reader.hasNextBatch()) {
      TsBlock tsblock = reader.nextBatch();
      cnt += tsblock.getPositionCount();
    }
    reader.close();
    // we cannot offer the exact number since when exactly ttl will be checked is unknown
    assertTrue(cnt <= 1000);

    DataNodeTTLCache.getInstance().setTTL(sg1, 1);
    dataSource =
        dataRegion.query(
            Collections.singletonList(mockMeasurementPath()),
            IDeviceID.Factory.DEFAULT_FACTORY.create(sg1),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());

    reader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(TEST_QUERY_JOB_ID),
            seqResource,
            unseqResource,
            true);

    cnt = 0;
    while (reader.hasNextBatch()) {
      TsBlock tsblock = reader.nextBatch();
      cnt += tsblock.getPositionCount();
    }
    reader.close();
    // we cannot offer the exact number since when exactly ttl will be checked is unknown
    assertTrue(cnt == 0);
  }

  private NonAlignedFullPath mockMeasurementPath() {
    return new NonAlignedFullPath(
        IDeviceID.Factory.DEFAULT_FACTORY.create(sg1),
        new MeasurementSchema(
            s1,
            TSDataType.INT64,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            Collections.emptyMap()));
  }

  @Test
  public void testTTLRemoval()
      throws StorageEngineException,
          WriteProcessException,
          IllegalPathException,
          InterruptedException {
    boolean isEnableCrossCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    prepareData();

    dataRegion.syncCloseAllWorkingTsFileProcessors();

    // files before ttl
    File seqDir = new File(TierManager.getInstance().getNextFolderForTsFile(0, true), sg1);
    File unseqDir = new File(TierManager.getInstance().getNextFolderForTsFile(0, false), sg1);

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
      sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    DataNodeTTLCache.getInstance().setTTL(sg1, 500);
    for (long timePartition : dataRegion.getTimePartitions()) {
      CompactionScheduler.tryToSubmitSettleCompactionTask(
          dataRegion.getTsFileManager(), timePartition, new CompactionScheduleContext(), true);
    }
    long totalWaitingTime = 0;
    while (dataRegion.getTsFileManager().getTsFileList(true).size()
            + dataRegion.getTsFileManager().getTsFileList(false).size()
        != 0) {
      sleep(200);
      totalWaitingTime += 200;
      if (totalWaitingTime >= 5000) {
        fail();
      }
    }

    // files after ttl
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
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(isEnableCrossCompaction);
  }

  @Test
  public void testParseSetTTL() {
    SetTTLStatement statement1 =
        (SetTTLStatement)
            StatementGenerator.createStatement(
                "SET TTL TO " + sg1 + " 10000", ZoneId.systemDefault());
    assertEquals(sg1, statement1.getPath().getFullPath());
    assertEquals(10000, statement1.getTTL());

    UnSetTTLStatement statement2 =
        (UnSetTTLStatement)
            StatementGenerator.createStatement("UNSET TTL TO " + sg2, ZoneId.systemDefault());
    assertEquals(sg2, statement2.getPath().getFullPath());
    assertEquals(TTLCache.NULL_TTL, statement2.getTTL());
  }

  @Test
  public void testParseShowTTL() {
    ShowTTLStatement statement1 =
        (ShowTTLStatement)
            StatementGenerator.createStatement("SHOW ALL TTL", ZoneId.systemDefault());
    assertEquals(1, statement1.getPaths().size());
    assertEquals("root.**", statement1.getPaths().get(0).getFullPath());
  }

  @Test
  public void testTTLCleanFile()
      throws WriteProcessException, IllegalPathException, InterruptedException {
    boolean isEnableCrossCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    prepareData();
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    assertEquals(4, dataRegion.getSequenceFileList().size());
    assertEquals(4, dataRegion.getUnSequenceFileList().size());

    DataNodeTTLCache.getInstance().setTTL(sg1, 1);
    for (long timePartition : dataRegion.getTimePartitions()) {
      CompactionScheduler.tryToSubmitSettleCompactionTask(
          dataRegion.getTsFileManager(), timePartition, new CompactionScheduleContext(), true);
    }
    long totalWaitingTime = 0;
    while (dataRegion.getTsFileManager().getTsFileList(true).size()
            + dataRegion.getTsFileManager().getTsFileList(false).size()
        != 0) {
      sleep(200);
      totalWaitingTime += 200;
      if (totalWaitingTime >= 5000) {
        fail();
      }
    }

    assertEquals(0, dataRegion.getSequenceFileList().size());
    assertEquals(0, dataRegion.getUnSequenceFileList().size());
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(isEnableCrossCompaction);
  }
}
