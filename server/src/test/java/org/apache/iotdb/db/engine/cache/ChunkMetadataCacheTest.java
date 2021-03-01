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
package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ChunkMetadataCacheTest {

  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  private String storageGroup = "root.vehicle.d0";
  private String measurementId0 = "s0";
  private String measurementId1 = "s1";
  private String measurementId2 = "s2";
  private String measurementId3 = "s3";
  private String measurementId4 = "s4";
  private String measurementId5 = "s5";
  private StorageGroupProcessor storageGroupProcessor;
  private String systemDir =
      TestConstant.BASE_OUTPUT_PATH.concat("data").concat(File.separator).concat("info");

  private int prevUnseqLevelNum = 0;

  @Before
  public void setUp() throws Exception {
    prevUnseqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(2);
    EnvironmentUtils.envSetUp();
    MetadataManagerHelper.initMetadata();
    storageGroupProcessor =
        new StorageGroupProcessor(systemDir, storageGroup, new DirectFlushPolicy(), storageGroup);
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    storageGroupProcessor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(systemDir);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnseqLevelNum);
  }

  private void insertOneRecord(long time, int num)
      throws WriteProcessException, IllegalPathException {
    TSRecord record = new TSRecord(time, storageGroup);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId0, String.valueOf(num)));
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT64, measurementId1, String.valueOf(num)));
    record.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, measurementId2, String.valueOf(num)));
    record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, measurementId3, String.valueOf(num)));
    record.addTuple(DataPoint.getDataPoint(TSDataType.BOOLEAN, measurementId4, "True"));
    InsertRowPlan insertRowPlan = new InsertRowPlan(record);
    storageGroupProcessor.insert(insertRowPlan);
  }

  protected void insertData() throws IOException, WriteProcessException, IllegalPathException {
    for (int j = 1; j <= 100; j++) {
      insertOneRecord(j, j);
    }
    for (TsFileProcessor tsFileProcessor :
        storageGroupProcessor.getWorkSequenceTsFileProcessors()) {
      tsFileProcessor.syncFlush();
    }

    for (int j = 10; j >= 1; j--) {
      insertOneRecord(j, j);
    }
    for (int j = 11; j <= 20; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 21; j <= 30; j += 2) {
      insertOneRecord(j, 0); // will be covered when read
    }
    storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 21; j <= 30; j += 2) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();

    insertOneRecord(2, 100);
  }

  @Test
  public void test1() throws IOException, QueryProcessException, IllegalPathException {
    IoTDBDescriptor.getInstance().getConfig().setMetaDataCacheEnable(false);
    QueryDataSource queryDataSource =
        storageGroupProcessor.query(
            new PartialPath(storageGroup), measurementId5, context, null, null);

    List<TsFileResource> seqResources = queryDataSource.getSeqResources();
    List<TsFileResource> unseqResources = queryDataSource.getUnseqResources();

    Assert.assertEquals(1, seqResources.size());
    Assert.assertEquals(3, unseqResources.size());
    Assert.assertTrue(seqResources.get(0).isClosed());
    Assert.assertTrue(unseqResources.get(0).isClosed());
    Assert.assertTrue(unseqResources.get(1).isClosed());
    Assert.assertTrue(unseqResources.get(2).isClosed());

    List<ChunkMetadata> metaDataList =
        ChunkMetadataCache.getInstance()
            .get(seqResources.get(0).getTsFilePath(), new Path(storageGroup, measurementId5), null);
    Assert.assertEquals(0, metaDataList.size());
  }

  @Test
  public void test2() throws IOException, QueryProcessException, IllegalPathException {
    IoTDBDescriptor.getInstance().getConfig().setMetaDataCacheEnable(true);
    QueryDataSource queryDataSource =
        storageGroupProcessor.query(
            new PartialPath(storageGroup), measurementId5, context, null, null);

    List<TsFileResource> seqResources = queryDataSource.getSeqResources();
    List<TsFileResource> unseqResources = queryDataSource.getUnseqResources();
    Assert.assertEquals(1, seqResources.size());
    Assert.assertEquals(3, unseqResources.size());
    Assert.assertTrue(seqResources.get(0).isClosed());
    Assert.assertTrue(unseqResources.get(0).isClosed());
    Assert.assertTrue(unseqResources.get(1).isClosed());
    Assert.assertTrue(unseqResources.get(2).isClosed());

    String seqTsFilePath = seqResources.get(0).getTsFilePath();
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(seqTsFilePath, true);
    Map<String, List<TimeseriesMetadata>> stringListMap = tsFileReader.getAllTimeseriesMetadata();
    List<TimeseriesMetadata> timeseriesMetadataList = stringListMap.get(storageGroup);

    ChunkMetadataCache chunkMetadataCache = ChunkMetadataCache.getInstance();
    chunkMetadataCache.clear();
    double hitRatio = chunkMetadataCache.calculateChunkMetaDataHitRatio();
    for (TimeseriesMetadata tsMd : timeseriesMetadataList) {
      chunkMetadataCache.get(seqTsFilePath, new Path(storageGroup, tsMd.getMeasurementId()), tsMd);
    }
    double decreasedHitRatio = chunkMetadataCache.calculateChunkMetaDataHitRatio();
    Assert.assertTrue("The hit rate should not increase", decreasedHitRatio <= hitRatio);

    // After an element is accessed, the newest element is put at the tail, so the head is the
    // eldest one
    TimeseriesMetadata tsMd2 = timeseriesMetadataList.get(2);
    chunkMetadataCache.get(seqTsFilePath, new Path(storageGroup, tsMd2.getMeasurementId()), tsMd2);

    String keyPrefix =
        seqTsFilePath + IoTDBConstant.PATH_SEPARATOR + storageGroup + IoTDBConstant.PATH_SEPARATOR;
    AccountableString key0 =
        new AccountableString(keyPrefix + timeseriesMetadataList.get(0).getMeasurementId());
    AccountableString key2 =
        new AccountableString(keyPrefix + timeseriesMetadataList.get(2).getMeasurementId());

    List<AccountableString> keyList =
        chunkMetadataCache.entrySet().stream().map(Entry::getKey).collect(Collectors.toList());
    Assert.assertEquals("Now, the eldest(head) key should be key0", keyList.get(0), key0);
    Assert.assertEquals("Now, the newest(tail) key should be key2", keyList.get(4), key2);

    double increasedHitRatio = ChunkMetadataCache.getInstance().calculateChunkMetaDataHitRatio();
    Assert.assertTrue("The hit rate should increase", decreasedHitRatio < increasedHitRatio);
  }
}
