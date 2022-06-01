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

package org.apache.iotdb.db.engine.merge;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

abstract class MergeTest {

  static final String MERGE_TEST_SG = "root.mergeTest";

  int seqFileNum = 5;
  int unseqFileNum = 5;
  int measurementNum = 10;
  int deviceNum = 10;
  // points of each series in a file
  long ptNum = 100;
  // flush each time "flushInterval" points of all series are written
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private int prevMergeChunkThreshold;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    IoTDB.metaManager.init();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(-1);
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    MergeManager.getINSTANCE().start();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles(seqResources, unseqResources);
    seqResources.clear();
    unseqResources.clear();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkThreshold);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    MergeManager.getINSTANCE().stop();
  }

  protected MeasurementSchema toMeasurementSchema(int measurementIndex) {
    return new MeasurementSchema(
        "sensor" + measurementIndex, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
  }

  protected String toDeviceId(int deviceIndex) {
    return MERGE_TEST_SG + PATH_SEPARATOR + "device" + deviceIndex;
  }

  protected void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] = toMeasurementSchema(i);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = toDeviceId(i);
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(MERGE_TEST_SG));
    createTimeseries(deviceIds, measurementSchemas);
  }

  protected void createTimeseries(String[] deviceIds, MeasurementSchema[] measurementSchemas)
      throws MetadataException {
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        IoTDB.metaManager.createTimeseries(
            devicePath.concatNode(measurementSchema.getMeasurementId()),
            measurementSchema.getType(),
            measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  protected String toFileName(int fileIndex) {
    return TestConstant.OUTPUT_DATA_DIR.concat(
        fileIndex
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + fileIndex
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + 0
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + 0
            + ".tsfile");
  }

  protected TsFileResource prepareResource(int fileIndex) {
    File file = new File(toFileName(fileIndex));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setMinPlanIndex(fileIndex);
    tsFileResource.setMaxPlanIndex(fileIndex);
    tsFileResource.setVersion(fileIndex);
    return tsFileResource;
  }

  protected void prepareSeqFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    prepareFile(tsFileResource, timeOffset, ptNum, valueOffset);
  }

  protected void prepareUnseqFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    prepareFile(tsFileResource, timeOffset, ptNum, valueOffset);
  }

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    // seq files, each has "ptNum" points of all series
    for (int i = 0; i < seqFileNum; i++) {
      TsFileResource tsFileResource = prepareResource(i);
      seqResources.add(tsFileResource);
      prepareSeqFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    // unseq files, each overlaps ONE seq file and latter ones has more points
    for (int i = 0; i < unseqFileNum; i++) {
      TsFileResource tsFileResource = prepareResource(seqFileNum + i);
      unseqResources.add(tsFileResource);
      prepareUnseqFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    if (unseqFileNum > 0) {
      // an additional unseq file that overlaps ALL seq files
      TsFileResource tsFileResource = prepareResource(seqFileNum + unseqFileNum);
      unseqResources.add(tsFileResource);
      prepareUnseqFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
    }
  }

  void removeFiles(List<TsFileResource> seqResList, List<TsFileResource> unseqResList)
      throws IOException {
    for (TsFileResource tsFileResource : seqResList) {
      tsFileResource.remove();
      tsFileResource.getModFile().remove();
    }
    for (TsFileResource tsFileResource : unseqResList) {
      tsFileResource.remove();
      tsFileResource.getModFile().remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    prepareFile(tsFileResource, timeOffset, ptNum, valueOffset, deviceIds, measurementSchemas);
  }

  void prepareFile(
      TsFileResource tsFileResource,
      long timeOffset,
      long ptNum,
      long valueOffset,
      String[] deviceIds,
      MeasurementSchema[] measurementSchemas)
      throws IOException, WriteProcessException {
    File tsfile = tsFileResource.getTsFile();
    if (!tsfile.getParentFile().exists()) {
      Assert.assertTrue(tsfile.getParentFile().mkdirs());
    }
    TsFileWriter fileWriter = new TsFileWriter(tsfile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (String deviceId : deviceIds) {
        TSRecord record = new TSRecord(i, deviceId);
        for (MeasurementSchema measurementSchema : measurementSchemas) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchema.getType(),
                  measurementSchema.getMeasurementId(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceId, i);
        tsFileResource.updateEndTime(deviceId, i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }

  @FunctionalInterface
  protected interface QueryResultChecker {
    void check(long time, Object value, int pointIndex);
  }

  @FunctionalInterface
  protected interface QueryCntChecker {
    void check(int cnt);
  }

  protected static QueryResultChecker checkResultFunc(long valueOffset) {
    return (time, value, index) -> assertEquals(time + valueOffset, (double) value, 0.001);
  }

  protected static QueryCntChecker checkResultCntFunc(long expected) {
    return cnt -> assertEquals(expected, cnt);
  }

  /**
   * Query all points of a timeseries and check point by point.
   *
   * @param deviceId of the queried timeseries
   * @param measurementSchema of the queried timeseries
   * @param resources files to be queried
   * @param resultChecker how should each point be examined
   * @param cntChecker how should the final count be examined
   * @throws IOException
   * @throws IllegalPathException
   */
  protected static void queryAndCheck(
      String deviceId,
      MeasurementSchema measurementSchema,
      List<TsFileResource> resources,
      QueryResultChecker resultChecker,
      QueryCntChecker cntChecker)
      throws IOException, IllegalPathException {
    QueryContext context = new QueryContext();
    PartialPath path = new PartialPath(deviceId, measurementSchema.getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchema.getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    int cnt = 0;
    try {
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        for (int i = 0; i < batchData.length(); i++) {
          long time = batchData.getTimeByIndex(i);
          Object value = batchData.getDoubleByIndex(i);
          resultChecker.check(time, value, i);
          cnt++;
        }
      }
      cntChecker.check(cnt);
    } finally {
      tsFilesReader.close();
    }
  }
}
