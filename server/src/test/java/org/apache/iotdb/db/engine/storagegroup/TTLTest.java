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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setTimePartitionInterval(86400000);
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
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setTimePartitionInterval(prevPartitionInterval);
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

    dataRegion.setDataTTL(1000);
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
            sg1,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null);
    List<TsFileResource> seqResource = dataSource.getSeqResources();
    List<TsFileResource> unseqResource = dataSource.getUnseqResources();
    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());

    dataRegion.setDataTTL(500);

    // files after ttl
    dataSource =
        dataRegion.query(
            Collections.singletonList(mockMeasurementPath()),
            sg1,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertTrue(seqResource.size() < 4);
    assertEquals(0, unseqResource.size());
    MeasurementPath path = mockMeasurementPath();

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

    dataRegion.setDataTTL(0);
    dataSource =
        dataRegion.query(
            Collections.singletonList(mockMeasurementPath()),
            sg1,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertEquals(0, seqResource.size());
    assertEquals(0, unseqResource.size());
  }

  private MeasurementPath mockMeasurementPath() throws MetadataException {
    return new MeasurementPath(
        new PartialPath(sg1 + TsFileConstant.PATH_SEPARATOR + s1),
        new MeasurementSchema(
            s1,
            TSDataType.INT64,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            Collections.emptyMap()));
  }

  @Test
  public void testTTLRemoval()
      throws StorageEngineException, WriteProcessException, QueryProcessException,
          IllegalPathException {
    prepareData();

    dataRegion.syncCloseAllWorkingTsFileProcessors();

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
    dataRegion.setDataTTL(500);
    dataRegion.checkFilesTTL();

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
  }

  @Test
  public void testParseSetTTL() {
    SetTTLStatement statement1 =
        (SetTTLStatement)
            StatementGenerator.createStatement(
                "SET TTL TO " + sg1 + " 10000", ZoneId.systemDefault());
    assertEquals(sg1, statement1.getStorageGroupPath().getFullPath());
    assertEquals(10000, statement1.getTTL());

    UnSetTTLStatement statement2 =
        (UnSetTTLStatement)
            StatementGenerator.createStatement("UNSET TTL TO " + sg2, ZoneId.systemDefault());
    assertEquals(sg2, statement2.getStorageGroupPath().getFullPath());
    assertEquals(Long.MAX_VALUE, statement2.getTTL());
  }

  @Test
  public void testParseShowTTL() {
    ShowTTLStatement statement1 =
        (ShowTTLStatement)
            StatementGenerator.createStatement("SHOW ALL TTL", ZoneId.systemDefault());
    assertTrue(statement1.getPaths().isEmpty());

    List<String> sgs = new ArrayList<>();
    sgs.add("root.sg1");
    sgs.add("root.sg2");
    sgs.add("root.sg3");
    ShowTTLStatement statement2 =
        (ShowTTLStatement)
            StatementGenerator.createStatement(
                "SHOW TTL ON root.sg1,root.sg2,root.sg3", ZoneId.systemDefault());
    assertEquals(
        sgs,
        statement2.getPaths().stream().map(PartialPath::getFullPath).collect(Collectors.toList()));
  }

  @Test
  public void testTTLCleanFile()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    prepareData();
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    assertEquals(4, dataRegion.getSequenceFileList().size());
    assertEquals(4, dataRegion.getUnSequenceFileList().size());

    dataRegion.setDataTTL(0);
    dataRegion.checkFilesTTL();

    assertEquals(0, dataRegion.getSequenceFileList().size());
    assertEquals(0, dataRegion.getUnSequenceFileList().size());
  }
}
