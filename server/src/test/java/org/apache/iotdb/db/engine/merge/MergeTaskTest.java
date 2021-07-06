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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MergeTaskTest extends MergeTest {

  private File tempSGDir;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testMerge() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testMergeEndTime() throws Exception {
    List<TsFileResource> testSeqResources = seqResources.subList(0, 3);
    List<TsFileResource> testUnseqResource = unseqResources.subList(5, 6);
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(testSeqResources, testUnseqResource),
            tempSGDir.getPath(),
            (k, v, l) -> {
              assertEquals(499, k.get(2).getEndTime("root.mergeTest.device1"));
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
  }

  @Test
  public void testFullMerge() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            true,
            10,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[9].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[9].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    long count = 0L;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int t = 0; t < batchData.length(); t++) {
        assertEquals(batchData.getTimeByIndex(t) + 20000.0, batchData.getDoubleByIndex(t), 0.001);
        count++;
      }
    }
    assertEquals(100, count);
    tsFilesReader.close();
  }

  @Test
  public void testChunkNumThreshold() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(Integer.MAX_VALUE);
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testPartialMerge1() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) < 20) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
        }
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testPartialMerge2() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources.subList(5, 6)),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testPartialMerge3() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources.subList(0, 5)),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(2));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) < 260) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
        }
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void mergeWithDeletionTest() throws Exception {
    try {
      PartialPath device = new PartialPath(deviceIds[0]);
      seqResources
          .get(0)
          .getModFile()
          .write(
              new Deletion(
                  device.concatNode(measurementSchemas[0].getMeasurementId()), 10000, 0, 49));
    } finally {
      seqResources.get(0).getModFile().close();
    }

    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            (k, v, l) -> {
              try {
                seqResources.get(0).removeModFile();
              } catch (IOException e) {
                e.printStackTrace();
              }
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) <= 20) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        }
        count++;
      }
    }
    assertEquals(70, count);
    tsFilesReader.close();
  }

  @Test
  public void testOnlyUnseqMerge() throws Exception {
    // unseq and no seq merge
    List<TsFileResource> testSeqResources = new ArrayList<>();
    List<TsFileResource> testUnseqResource = unseqResources.subList(5, 6);
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(testSeqResources, testUnseqResource),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(2));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  /**
   * merge 3 seqFile and 1 unseqFile seqFile1: d1.s1:0-100 d1.s2:0-100 seqFile2: d1.s1:100-200
   * seqFile3: d2.s1:0-100 unseqFile1: d1.s3:0-100
   */
  @Test
  public void testMergeWithSeqFileMissSomeSensorAndDevice() throws Exception {
    List<TsFileResource> testSeqResources = new ArrayList<>();
    List<TsFileResource> testUnseqResources = new ArrayList<>();

    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                100
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 100
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource seqTsFile1 = new TsFileResource(file);
    Map<Pair<String, MeasurementSchema>, Pair<Long, Long>> seqTsFile1Data = new HashMap<>();
    seqTsFile1Data.put(new Pair<>(deviceIds[0], measurementSchemas[0]), new Pair<>(0L, 100L));
    seqTsFile1Data.put(new Pair<>(deviceIds[0], measurementSchemas[1]), new Pair<>(0L, 100L));
    prepareFileWithSensorAndTime(seqTsFile1, seqTsFile1Data);
    testSeqResources.add(seqTsFile1);

    file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                101
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 101
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource seqTsFile2 = new TsFileResource(file);
    Map<Pair<String, MeasurementSchema>, Pair<Long, Long>> seqTsFileData2 = new HashMap<>();
    seqTsFileData2.put(new Pair<>(deviceIds[0], measurementSchemas[0]), new Pair<>(100L, 200L));
    prepareFileWithSensorAndTime(seqTsFile2, seqTsFileData2);
    testSeqResources.add(seqTsFile2);

    file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                102
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 102
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource seqTsFile3 = new TsFileResource(file);
    Map<Pair<String, MeasurementSchema>, Pair<Long, Long>> seqTsFileData3 = new HashMap<>();
    seqTsFileData3.put(new Pair<>(deviceIds[1], measurementSchemas[0]), new Pair<>(0L, 100L));
    prepareFileWithSensorAndTime(seqTsFile3, seqTsFileData3);
    testSeqResources.add(seqTsFile3);

    file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource unseqTsFile = new TsFileResource(file);
    Map<Pair<String, MeasurementSchema>, Pair<Long, Long>> unseqTsFileData = new HashMap<>();
    unseqTsFileData.put(new Pair<>(deviceIds[0], measurementSchemas[2]), new Pair<>(0L, 100L));
    prepareFileWithSensorAndTime(unseqTsFile, unseqTsFileData);
    testUnseqResources.add(unseqTsFile);

    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(testSeqResources, testUnseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {
              try (TsFileSequenceReader reader =
                  new TsFileSequenceReader(k.get(2).getTsFilePath())) {
                List<ChunkMetadata> chunkMetadataList =
                    reader.getChunkMetadataList(
                        new PartialPath(deviceIds[0], measurementSchemas[2].getMeasurementId()));
                assertEquals(1, chunkMetadataList.size());
              } catch (IOException | IllegalPathException e) {
                e.printStackTrace();
                fail();
              }
              for (TsFileResource tsFileResource : k) {
                tsFileResource.remove();
              }
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
  }

  /**
   * @param tsFileResource The File to write
   * @param generateMap map((device, measurement),(startTime, pointNum))
   */
  private void prepareFileWithSensorAndTime(
      TsFileResource tsFileResource,
      Map<Pair<String, MeasurementSchema>, Pair<Long, Long>> generateMap)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (Pair<String, MeasurementSchema> measurementSchemaPair : generateMap.keySet()) {
      fileWriter.registerTimeseries(
          new Path(measurementSchemaPair.left, measurementSchemaPair.right.getMeasurementId()),
          measurementSchemaPair.right);
    }

    for (Entry<Pair<String, MeasurementSchema>, Pair<Long, Long>> generateEntry :
        generateMap.entrySet()) {
      Pair<String, MeasurementSchema> measurementSchemaPair = generateEntry.getKey();
      String device = measurementSchemaPair.left;
      MeasurementSchema measurementSchema = measurementSchemaPair.right;
      Pair<Long, Long> startTimePointNumPair = generateEntry.getValue();
      for (long i = 0; i < startTimePointNumPair.right; i++) {
        TSRecord record = new TSRecord(i, device);
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchema.getType(),
                measurementSchema.getMeasurementId(),
                String.valueOf(i + startTimePointNumPair.left)));
        fileWriter.write(record);
        tsFileResource.updateStartTime(device, i);
        tsFileResource.updateEndTime(device, i);
        if ((i + 1) % flushInterval == 0) {
          fileWriter.flushAllChunkGroups();
        }
      }
    }
    fileWriter.close();
  }
}
