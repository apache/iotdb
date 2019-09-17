/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.engine.storagegroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.OutOfTTLException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.TTLPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TTLTest {

  private String sg1 = "root.TTL_SG1";
  private String sg2 = "root.TTL_SG2";
  private long ttl = 12345;
  private StorageGroupProcessor storageGroupProcessor;
  private String s1 = "s1";
  private String g1s1 = sg1 + IoTDBConstant.PATH_SEPARATOR + s1;

  @Before
  public void setUp()
      throws MetadataErrorException, ProcessorException, IOException, StartupException {
    EnvironmentUtils.envSetUp();
    createSchemas();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    storageGroupProcessor.waitForAllCurrentTsFileProcessorsClosed();
    EnvironmentUtils.cleanEnv();
  }

  private void createSchemas() throws MetadataErrorException, ProcessorException {
    MManager.getInstance().setStorageLevelToMTree(sg1);
    MManager.getInstance().setStorageLevelToMTree(sg2);
    storageGroupProcessor = new StorageGroupProcessor(IoTDBDescriptor.getInstance().getConfig()
        .getSystemDir(), sg1);
    MManager.getInstance().addPathToMTree(g1s1, TSDataType.INT64, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, Collections.emptyMap());
    storageGroupProcessor.addMeasurement("s1", TSDataType.INT64, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, Collections.emptyMap());
  }

  @Test
  public void testSetMetaTTL() throws IOException, PathErrorException {
    // exception is expected when setting ttl to a non-exist storage group
    boolean caught = false;
    try {
      MManager.getInstance().setTTL(sg1 + ".notExist", ttl);
    } catch (PathErrorException e) {
      caught = true;
    }
    assertTrue(caught);

    // normally set ttl
    MManager.getInstance().setTTL(sg1, ttl);
    MNode mNode = MManager.getInstance().getNodeByPathFromCache(sg1);
    assertEquals(ttl, mNode.getDataTTL());

    // default ttl
    mNode = MManager.getInstance().getNodeByPathFromCache(sg2);
    assertEquals(Long.MAX_VALUE, mNode.getDataTTL());
  }

  @Test
  public void testTTLWrite() throws StorageEngineException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(sg1);
    insertPlan.setTime(System.currentTimeMillis());
    insertPlan.setMeasurements(new String[]{"s1"});
    insertPlan.setValues(new String[]{"1"});
    insertPlan.setDataTypes(new TSDataType[]{TSDataType.INT64});

    // ok without ttl
    assertTrue(storageGroupProcessor.insert(insertPlan));

    storageGroupProcessor.setDataTTL(1000);
    // with ttl
    insertPlan.setTime(System.currentTimeMillis() - 1001);
    boolean caught = false;
    try {
      storageGroupProcessor.insert(insertPlan);
    } catch (OutOfTTLException e) {
      caught = true;
    }
    assertTrue(caught);
    insertPlan.setTime(System.currentTimeMillis() - 900);
    assertTrue(storageGroupProcessor.insert(insertPlan));
  }

  private void prepareData() throws StorageEngineException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(sg1);
    insertPlan.setTime(System.currentTimeMillis());
    insertPlan.setMeasurements(new String[]{"s1"});
    insertPlan.setValues(new String[]{"1"});
    insertPlan.setDataTypes(new TSDataType[]{TSDataType.INT64});

    long initTime = System.currentTimeMillis();
    // sequence data
    for (int i = 1000; i < 2000; i++) {
      insertPlan.setTime(initTime - 2000 + i);
      assertTrue(storageGroupProcessor.insert(insertPlan));
      if ((i + 1) % 300 == 0) {
        storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();
      }
    }
    // unsequence data
    for (int i = 0; i < 1000; i++) {
      insertPlan.setTime(initTime - 2000 + i);
      storageGroupProcessor.insert(insertPlan);
      if ((i + 1) % 300 == 0) {
        storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();
      }
    }
  }

  @Test
  public void testTTLRead() throws IOException, StorageEngineException {
    prepareData();

    storageGroupProcessor.merge(true);

    // files before ttl
    QueryDataSource dataSource = storageGroupProcessor.query(sg1, s1, EnvironmentUtils.TEST_QUERY_CONTEXT
        , null);
    List<TsFileResource> seqResource = dataSource.getSeqResources();
    List<TsFileResource> unseqResource = dataSource.getUnseqResources();
    assertEquals(4, seqResource.size());
    assertEquals(4, unseqResource.size());

    storageGroupProcessor.setDataTTL(500);

    // files after ttl
    dataSource = storageGroupProcessor.query(sg1, s1, EnvironmentUtils.TEST_QUERY_CONTEXT
        , null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertTrue(seqResource.size() < 4);
    assertEquals(0, unseqResource.size());
    Path path = new Path(sg1, s1);
    SeqResourceIterateReader reader = new SeqResourceIterateReader(path,
        seqResource, null, EnvironmentUtils.TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (reader.hasNext()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasNext()) {
        batchData.next();
        cnt ++;
      }
    }
    reader.close();
    // we cannot offer the exact number since when exactly ttl will be checked is unknown
    assertTrue(cnt <= 1000);

    storageGroupProcessor.setDataTTL(0);
    dataSource = storageGroupProcessor.query(sg1, s1, EnvironmentUtils.TEST_QUERY_CONTEXT
        , null);
    seqResource = dataSource.getSeqResources();
    unseqResource = dataSource.getUnseqResources();
    assertEquals(0, seqResource.size());
    assertEquals(0, unseqResource.size());


    QueryResourceManager.getInstance().endQueryForGivenJob(EnvironmentUtils.TEST_QUERY_JOB_ID);
  }

  @Test
  public void testTTLRemoval() throws StorageEngineException {
    prepareData();

    storageGroupProcessor.waitForAllCurrentTsFileProcessorsClosed();

    // files before ttl
    File seqDir = new File(DirectoryManager.getInstance().getNextFolderForSequenceFile(), sg1);
    File unseqDir = new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(), sg1);
    File[] seqFiles = seqDir.listFiles(f -> f.getName().endsWith(TsFileConstant.TSFILE_SUFFIX));
    File[] unseqFiles = unseqDir.listFiles(f -> f.getName().endsWith(TsFileConstant.TSFILE_SUFFIX));
    for (File file : seqFiles) {
      System.out.println(file.getPath());
    }
    assertEquals(4, seqFiles.length);
    assertEquals(4, unseqFiles.length);

    storageGroupProcessor.setDataTTL(500);
    storageGroupProcessor.checkFilesTTL();

    // files after ttl
    seqFiles = seqDir.listFiles(f -> f.getName().endsWith(TsFileConstant.TSFILE_SUFFIX));
    unseqFiles = unseqDir.listFiles(f -> f.getName().endsWith(TsFileConstant.TSFILE_SUFFIX));
    assertTrue(seqFiles.length <= 2);
    assertEquals(0, unseqFiles.length);
  }

  @Test
  public void testParseTTL()
      throws ArgsErrorException, MetadataErrorException, QueryProcessorException {
    QueryProcessor queryProcessor = new QueryProcessor(new QueryProcessExecutor());
    TTLPlan plan = (TTLPlan) queryProcessor.parseSQLToPhysicalPlan("SET TTL TO " + sg1 + " 10000");
    assertEquals(sg1, plan.getStorageGroup());
    assertEquals(10000, plan.getDataTTL());

    plan = (TTLPlan) queryProcessor.parseSQLToPhysicalPlan("UNSET TTL TO " + sg2);
    assertEquals(sg2, plan.getStorageGroup());
    assertEquals(Long.MAX_VALUE, plan.getDataTTL());
  }

}