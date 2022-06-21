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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaTestUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TTLTest {

  private String sg1 = "root.TTL_SG1";
  private String sg2 = "root.TTL_SG2";
  private long ttl = 12345;
  private DataRegion dataRegion;
  private String s1 = "s1";
  private String g1s1 = sg1 + IoTDBConstant.PATH_SEPARATOR + s1;
  private long prevPartitionInterval;

  @Before
  public void setUp() throws MetadataException, DataRegionException {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.envSetUp();
    createSchemas();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
  }

  private void createSchemas() throws MetadataException, DataRegionException {
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sg1));
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath(sg2));
    dataRegion =
        new DataRegion(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            sg1,
            new DirectFlushPolicy(),
            sg1);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath(g1s1),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        Collections.emptyMap());
  }

  @Test
  public void testSetMetaTTL() throws IOException, MetadataException {
    // exception is expected when setting ttl to a non-exist storage group
    boolean caught = false;

    try {
      IoTDB.schemaProcessor.setTTL(new PartialPath(sg1 + ".notExist"), ttl);
    } catch (MetadataException e) {
      caught = true;
    }
    assertTrue(caught);

    // normally set ttl
    IoTDB.schemaProcessor.setTTL(new PartialPath(sg1), ttl);
    IStorageGroupMNode mNode =
        IoTDB.schemaProcessor.getStorageGroupNodeByPath(new PartialPath(sg1));
    assertEquals(ttl, mNode.getDataTTL());

    // default ttl
    mNode = IoTDB.schemaProcessor.getStorageGroupNodeByPath(new PartialPath(sg2));
    assertEquals(Long.MAX_VALUE, mNode.getDataTTL());
  }

  @Test
  public void testTTLWrite()
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

    // ok without ttl
    dataRegion.insert(plan);

    dataRegion.setDataTTL(1000);
    // with ttl
    plan.setTime(System.currentTimeMillis() - 1001);
    boolean caught = false;
    try {
      dataRegion.insert(plan);
    } catch (OutOfTTLException e) {
      caught = true;
    }
    assertTrue(caught);
    plan.setTime(System.currentTimeMillis() - 900);
    dataRegion.insert(plan);
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
      dataRegion.insert(plan);
      if ((i + 1) % 300 == 0) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      }
    }
    // unsequence data
    for (int i = 0; i < 1000; i++) {
      plan.setTime(initTime - 2000 + i);
      dataRegion.insert(plan);
      if ((i + 1) % 300 == 0) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  @Test
  public void testTTLRead()
      throws IOException, WriteProcessException, StorageEngineException, QueryProcessException,
          MetadataException {
    prepareData();

    // files before ttl
    QueryDataSource dataSource =
        dataRegion.query(
            Collections.singletonList(
                SchemaTestUtils.getMeasurementPath(sg1 + TsFileConstant.PATH_SEPARATOR + s1)),
            sg1,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    List<TsFileResource> seqResource = dataSource.getSeqResources();
    List<TsFileResource> unseqResource = dataSource.getUnseqResources();
    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());

    dataRegion.setDataTTL(500);

    // files after ttl
    dataSource =
        dataRegion.query(
            Collections.singletonList(
                SchemaTestUtils.getMeasurementPath(sg1 + TsFileConstant.PATH_SEPARATOR + s1)),
            sg1,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertTrue(seqResource.size() < 4);
    assertEquals(0, unseqResource.size());
    MeasurementPath path =
        SchemaTestUtils.getMeasurementPath(sg1 + TsFileConstant.PATH_SEPARATOR + s1);
    Set<String> allSensors = new HashSet<>();
    allSensors.add(s1);
    IBatchReader reader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResource,
            unseqResource,
            null,
            null,
            true);

    int cnt = 0;
    while (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        batchData.next();
        cnt++;
      }
    }
    reader.close();
    // we cannot offer the exact number since when exactly ttl will be checked is unknown
    assertTrue(cnt <= 1000);

    dataRegion.setDataTTL(0);
    dataSource =
        dataRegion.query(
            Collections.singletonList(
                SchemaTestUtils.getMeasurementPath(sg1 + TsFileConstant.PATH_SEPARATOR + s1)),
            sg1,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertEquals(0, seqResource.size());
    assertEquals(0, unseqResource.size());
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
  public void testParseSetTTL() throws QueryProcessException {
    Planner planner = new Planner();
    SetTTLPlan plan = (SetTTLPlan) planner.parseSQLToPhysicalPlan("SET TTL TO " + sg1 + " 10000");
    assertEquals(sg1, plan.getStorageGroup().getFullPath());
    assertEquals(10000, plan.getDataTTL());

    plan = (SetTTLPlan) planner.parseSQLToPhysicalPlan("UNSET TTL TO " + sg2);
    assertEquals(sg2, plan.getStorageGroup().getFullPath());
    assertEquals(Long.MAX_VALUE, plan.getDataTTL());
  }

  @Test
  public void testParseShowTTL() throws QueryProcessException {
    Planner planner = new Planner();
    ShowTTLPlan plan = (ShowTTLPlan) planner.parseSQLToPhysicalPlan("SHOW ALL TTL");
    assertTrue(plan.getStorageGroups().isEmpty());

    List<String> sgs = new ArrayList<>();
    sgs.add("root.sg1");
    sgs.add("root.sg2");
    sgs.add("root.sg3");
    plan = (ShowTTLPlan) planner.parseSQLToPhysicalPlan("SHOW TTL ON root.sg1,root.sg2,root.sg3");
    assertEquals(
        sgs,
        plan.getStorageGroups().stream()
            .map(PartialPath::getFullPath)
            .collect(Collectors.toList()));
  }

  @Test
  public void testShowTTL()
      throws IOException, QueryProcessException, QueryFilterOptimizationException,
          StorageEngineException, MetadataException, InterruptedException {
    IoTDB.schemaProcessor.setTTL(new PartialPath(sg1), ttl);

    ShowTTLPlan plan = new ShowTTLPlan(Collections.emptyList());
    PlanExecutor executor = new PlanExecutor();
    QueryDataSet queryDataSet = executor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      String sg = rowRecord.getFields().get(0).getStringValue();
      if (sg.equals(sg1)) {
        assertEquals(ttl, rowRecord.getFields().get(1).getLongV());
      } else if (sg.equals(sg2)) {
        assertNull(rowRecord.getFields().get(1));
      } else {
        fail();
      }
    }
  }

  @Test
  public void testTTLCleanFile()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
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
