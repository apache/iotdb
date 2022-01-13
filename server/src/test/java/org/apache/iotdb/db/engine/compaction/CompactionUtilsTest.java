/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class CompactionUtilsTest extends AbstractCompactionTest {
  private String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    Thread.currentThread().setName("IoTB-pool-Compaction-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
  }

  /* Total 5 seq files, each file has the same 6 nonAligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, false, true);

    PartialPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1",
            "s1",
            new MeasurementSchema("s1", TSDataType.INT64));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResources,
            unseqResources,
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        assertEquals(batchData.currentTime(), batchData.currentValue());
        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            targetResources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        assertEquals(batchData.currentTime(), batchData.currentValue());
        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }

  /*
  Total 6 seq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s4, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  */
  @Test
  public void testSeqInnerSpaceCompactionWithDifferentTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(5, 5, false);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    createFiles(2, 5, 5, 50, 600, 800, 50, 50, false, true);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < 3) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < 3 && j < 5) {
          assertEquals(200, count);
        } else if (i < 5 && j < 5) {
          assertEquals(100, count);
        }
      }
    }
  }

  /* Total 5 unseq files, each file has the same 6 nonAligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testUnSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, false, false);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s1",
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(batchData.currentTime(), batchData.currentValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s1",
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(batchData.currentTime(), batchData.currentValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
  }

  /*
  Total 10 seq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 150 ~ 199 and 250 ~ 299, value range is 150 ~ 199 and 250 ~ 299.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s4, time range is 100 ~ 149 and 250 ~ 299, value range is 100 ~ 149 and 250 ~ 299.
  Seventh and Eighth file: d0 ~ d6 and s0 ~ s6, time range is 200 ~ 269 and 370 ~ 439, value range is 300 ~ 369 and 470 ~ 539.
  Ninth and Tenth file: d0 ~ d8 and s0 ~ s8, time range is 100 ~ 169 and 270 ~ 339, value range is 300 ~ 369 and 470 ~ 539.
  */
  @Test
  public void testUnSeqInnerSpaceCompactionWithDifferentTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(9, 9, false);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, false);
    createFiles(2, 3, 5, 50, 150, 150, 50, 50, false, false);
    createFiles(2, 5, 5, 50, 100, 100, 100, 100, false, false);
    createFiles(2, 7, 7, 70, 200, 300, 100, 100, false, false);
    createFiles(2, 9, 9, 70, 100, 300, 100, 100, false, false);

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if ((100 <= batchData.currentTime() && batchData.currentTime() < 170)
                || (270 <= batchData.currentTime() && batchData.currentTime() < 340)) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else if ((200 <= batchData.currentTime() && batchData.currentTime() < 270)
                || (370 <= batchData.currentTime() && batchData.currentTime() < 440)) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(410, count);
        } else if (i < 3 && j < 5) {
          assertEquals(310, count);
        } else if (i < 5 && j < 5) {
          assertEquals(280, count);
        } else if (i < 7 && j < 7) {
          assertEquals(280, count);
        } else {
          assertEquals(140, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if ((100 <= batchData.currentTime() && batchData.currentTime() < 170)
                || (270 <= batchData.currentTime() && batchData.currentTime() < 340)) {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            } else if ((200 <= batchData.currentTime() && batchData.currentTime() < 270)
                || (370 <= batchData.currentTime() && batchData.currentTime() < 440)) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(410, count);
        } else if (i < 3 && j < 5) {
          assertEquals(310, count);
        } else if (i < 5 && j < 5) {
          assertEquals(280, count);
        } else if (i < 7 && j < 7) {
          assertEquals(280, count);
        } else {
          assertEquals(140, count);
        }
      }
    }
  }

  /*
  Total 6 unseq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 300 ~ 599 , value range is  0 ~ 299 and 300 ~ 599.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 200 ~ 499 and 550 ~ 849, value range is 300 ~ 599 and 650 ~ 949.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 900 ~ 1199 and 1250 ~ 1549, value range is 1100 ~ 1399 and 1450 ~ 1749.
  The data of d0.s0, d0.s1, d2.s4 and d3.s5 is deleted in each file.
  */
  @Test
  public void testUnSeqInnerSpaceCompactionWithAllDataDeletedInTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, false);
    createFiles(2, 2, 3, 300, 0, 0, 0, 0, false, false);
    createFiles(2, 3, 5, 300, 200, 300, 50, 50, false, false);
    createFiles(2, 5, 7, 300, 900, 1100, 50, 50, false, false);

    // generate mods file
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + 0 + PATH_SEPARATOR + "s0",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + 0 + PATH_SEPARATOR + "s1",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + 2 + PATH_SEPARATOR + "s4",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      deleteMap.put(
          COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + 3 + PATH_SEPARATOR + "s5",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == 0 && j == 0) || (i == 0 && j == 1) || (i == 2 && j == 4) || (i == 3 && j == 5)) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == 0 && j == 0) || (i == 0 && j == 1) || (i == 2 && j == 4) || (i == 3 && j == 5)) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /*
  Total 6 unseq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 300 ~ 599 , value range is  0 ~ 299 and 300 ~ 599.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 200 ~ 499 and 550 ~ 849, value range is 300 ~ 599 and 650 ~ 949.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 900 ~ 1199 and 1250 ~ 1549, value range is 1100 ~ 1399 and 1450 ~ 1749.
  The data of device d0 is deleted in each file.
  */
  @Test
  public void testUnSeqInnerSpaceCompactionWithAllDataDeletedInDevice()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, false);
    createFiles(2, 2, 3, 300, 0, 0, 0, 0, false, false);
    createFiles(2, 3, 5, 300, 200, 300, 50, 50, false, false);
    createFiles(2, 5, 7, 300, 900, 1100, 50, 50, false, false);

    // generate mods file
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (int j = 0; j < 7; j++) {
        deleteMap.put(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + 0 + PATH_SEPARATOR + "s" + j,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(batchData.currentTime() + 100, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime() + 200, batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /*
  Total 6 unseq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 300 ~ 599 , value range is  0 ~ 299 and 300 ~ 599.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 200 ~ 499 and 550 ~ 849, value range is 300 ~ 599 and 650 ~ 949.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 900 ~ 1199 and 1250 ~ 1549, value range is 1100 ~ 1399 and 1450 ~ 1749.
  The data of device d0 ~ d4 is deleted in each file.
  */
  @Test
  public void testUnSeqInnerSpaceCompactionWithAllDataDeletedInTargetFile()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, false);
    createFiles(2, 2, 3, 300, 0, 0, 0, 0, false, false);
    createFiles(2, 3, 5, 300, 200, 300, 50, 50, false, false);
    createFiles(2, 5, 7, 300, 900, 1100, 50, 50, false, false);

    // generate mods file
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (int d = 0; d < 5; d++) {
        for (int j = 0; j < 7; j++) {
          deleteMap.put(
              COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + d + PATH_SEPARATOR + "s" + j,
              new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
        }
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(0, count);
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(0, count);
      }
    }
  }

  /* Total 5 seq files, each file has the same 6 aligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, true, true);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(
                batchData.currentTime(),
                ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(
                batchData.currentTime(),
                ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
  }

  /*
  Total 6 seq files, each file has different aligned timeseries, which cause empty page.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  */
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithDifferentTimeseriesAndEmptyPage()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(50);
    registerTimeseriesInMManger(5, 7, true);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, true, true);
    createFiles(2, 5, 7, 50, 600, 800, 50, 50, true, true);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
  }

  /*
  Total 6 seq files, each file has different aligned timeseries, which cause empty value chunk.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  */
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithDifferentTimeseriesAndEmptyChunk()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(5, 7, true);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, true, true);
    createFiles(2, 5, 7, 50, 600, 800, 50, 50, true, true);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                false);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() >= 600) {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }
  }

  /*
  Total 6 unseq files, each file has different aligned timeseries, which cause empty page and chunk.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 300 ~ 599 , value range is  0 ~ 299 and 300 ~ 599.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 200 ~ 499 and 550 ~ 849, value range is 300 ~ 599 and 650 ~ 949.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 900 ~ 1199 and 1250 ~ 1549, value range is 1100 ~ 1399 and 1450 ~ 1749.
  */
  @Test
  public void testAlignedUnSeqInnerSpaceCompactionWithEmptyChunkAndEmptyPage()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, true);
    createFiles(2, 2, 3, 300, 0, 0, 0, 0, true, false);
    createFiles(2, 3, 5, 300, 200, 300, 50, 50, true, false);
    createFiles(2, 5, 7, 300, 900, 1100, 50, 50, true, false);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(
                  batchData.currentTime() + 100,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(
                  batchData.currentTime() + 100,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /*
  Total 6 unseq files, each file has different aligned timeseries, which cause empty page and chunk.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 300 ~ 599 , value range is  0 ~ 299 and 300 ~ 599.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 200 ~ 499 and 550 ~ 849, value range is 300 ~ 599 and 650 ~ 949.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 900 ~ 1199 and 1250 ~ 1549, value range is 1100 ~ 1399 and 1450 ~ 1749.
  The data of d0.s0, d0.s1, d2.s4 and d3.s5 is deleted in each file.
  */
  @Test
  public void testAlignedUnSeqInnerSpaceCompactionWithAllDataDeletedInTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, true);
    createFiles(2, 2, 3, 300, 0, 0, 0, 0, true, false);
    createFiles(2, 3, 5, 300, 200, 300, 50, 50, true, false);
    createFiles(2, 5, 7, 300, 900, 1100, 50, 50, true, false);

    // generate mods file
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      deleteMap.put(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + TsFileGeneratorUtils.getAlignDeviceOffset()
              + PATH_SEPARATOR
              + "s0",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      deleteMap.put(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + TsFileGeneratorUtils.getAlignDeviceOffset()
              + PATH_SEPARATOR
              + "s1",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      deleteMap.put(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)
              + PATH_SEPARATOR
              + "s4",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      deleteMap.put(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
              + PATH_SEPARATOR
              + "s5",
          new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(
                  batchData.currentTime() + 100,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 0)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j == 4)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j == 5)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(
                  batchData.currentTime() + 100,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 0)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j == 4)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j == 5)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /*
  Total 6 unseq files, each file has different aligned timeseries, which cause empty page and chunk.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 300 ~ 599 , value range is  0 ~ 299 and 300 ~ 599.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 200 ~ 499 and 550 ~ 849, value range is 300 ~ 599 and 650 ~ 949.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s6, time range is 900 ~ 1199 and 1250 ~ 1549, value range is 1100 ~ 1399 and 1450 ~ 1749.
  The data of device d0 is deleted in each file.
  */
  @Test
  public void testAlignedUnSeqInnerSpaceCompactionWithAllDataDeletedInDevice()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, true);
    createFiles(2, 2, 3, 300, 0, 0, 0, 0, true, false);
    createFiles(2, 3, 5, 300, 200, 300, 50, 50, true, false);
    createFiles(2, 5, 7, 300, 900, 1100, 50, 50, true, false);

    // generate mods file
    for (int i = 0; i < unseqResources.size(); i++) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (int j = 0; j < 7; j++) {
        deleteMap.put(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + TsFileGeneratorUtils.getAlignDeviceOffset()
                + PATH_SEPARATOR
                + "s"
                + j,
            new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, unseqResources.get(i), false);
    }

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(
                  batchData.currentTime() + 100,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == TsFileGeneratorUtils.getAlignDeviceOffset()) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (batchData.currentTime() < 200
                || (batchData.currentTime() < 550 && batchData.currentTime() >= 500)) {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if (batchData.currentTime() < 850) {
              assertEquals(
                  batchData.currentTime() + 100,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime() + 200,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == TsFileGeneratorUtils.getAlignDeviceOffset()) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /* Total 5 files, each file has the same 6 aligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testAlignedUnSeqInnerSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, true, false);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(
                batchData.currentTime(),
                ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, true, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                new ArrayList<>(),
                targetResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            assertEquals(
                batchData.currentTime(),
                ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        assertEquals(500, count);
      }
    }
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has the same nonAligned timeseries
   *
   * <p>Seq files has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99, 100 ~ 199, 200 ~ 299, 300 ~ 399 and
   * 400 ~ 499, value range is 0 ~ 99, 100 ~ 199, 200 ~ 299, 300 ~ 399 and 400 ~ 499.
   *
   * <p>UnSeq files has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 49, 100 ~ 149, 200 ~ 249, 300 ~ 349
   * and 400 ~ 449, value range is 10000 ~ 10049, 10100 ~ 10149, 10200 ~ 10249, 10300 ~ 10349 and
   * 10400 ~ 10449.
   */
  @Test
  public void testCrossSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    PartialPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1",
            "s1",
            new MeasurementSchema("s1", TSDataType.INT64));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResources,
            unseqResources,
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
        } else {
          assertEquals(batchData.currentTime(), batchData.currentValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            targetResources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
        } else {
          assertEquals(batchData.currentTime(), batchData.currentValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   */
  @Test
  public void testCrossSpaceCompactionWithDifferentTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0.s0, d0.s1, d2.s4 and d3.s4 is deleted in each file.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s0");
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s1");
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s4");
    seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3" + PATH_SEPARATOR + "s4");
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == 0 && j == 0) || (i == 0 && j == 1) || (i == 2 && j == 4) || (i == 3 && j == 4)) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == 0 && j == 0) || (i == 0 && j == 1) || (i == 2 && j == 4) || (i == 3 && j == 4)) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == 0) {
          assertEquals(800, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0 and d2 is deleted in each file.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInDevice()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 2) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 2) {
          assertEquals(0, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1280, count);
        } else if ((i == 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < 3) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0, d1 and d2 is deleted in each file. Data in the first target file is all
   * deleted.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInOneTargetFile()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0, d1 and d2 is deleted in each seq file.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInDeviceInSeqFiles()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 1) {
          if (j < 4) {
            assertEquals(630, count);
          } else {
            assertEquals(200, count);
          }
        } else if (i < 3) {
          if (j < 4) {
            assertEquals(600, count);
          } else {
            assertEquals(0, count);
          }
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.INT64,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == 0
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(batchData.currentTime() + 20000, batchData.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            } else {
              assertEquals(batchData.currentTime(), batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < 1) {
          if (j < 4) {
            assertEquals(630, count);
          } else {
            assertEquals(200, count);
          }
        } else if (i < 3) {
          if (j < 4) {
            assertEquals(600, count);
          } else {
            assertEquals(0, count);
          }
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 5 seq files and 5 unseq files, each file has the same aligned timeseries
   *
   * <p>Seq files has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99, 100 ~ 199, 200 ~ 299, 300 ~ 399 and
   * 400 ~ 499, value range is 0 ~ 99, 100 ~ 199, 200 ~ 299, 300 ~ 399 and 400 ~ 499.
   *
   * <p>UnSeq files has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 49, 100 ~ 149, 200 ~ 249, 300 ~ 349
   * and 400 ~ 449, value range is 10000 ~ 10049, 10100 ~ 10149, 10200 ~ 10249, 10300 ~ 10349 and
   * 10400 ~ 10449.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithSameTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, true, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, true, false);

    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s1", TSDataType.INT64));
    AlignedPath path =
        new AlignedPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000",
            Collections.singletonList("s1"),
            schemas);
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.VECTOR,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            seqResources,
            unseqResources,
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(
              batchData.currentTime() + 10000,
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        } else {
          assertEquals(
              batchData.currentTime(),
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        }
        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            TSDataType.INT64,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            targetResources,
            new ArrayList<>(),
            null,
            null,
            true);
    count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      while (batchData.hasCurrent()) {
        if (batchData.currentTime() % 100 < 50) {
          assertEquals(
              batchData.currentTime() + 10000,
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        } else {
          assertEquals(
              batchData.currentTime(),
              ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
        }

        count++;
        batchData.next();
      }
    }
    tsFilesReader.close();
    assertEquals(500, count);
  }

  /**
   * Total 4 seq files and 4 unseq files, each file has different aligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithDifferentTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, true);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, true, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, true, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, true, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, true, false);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == TsFileGeneratorUtils.getAlignDeviceOffset()) {
          assertEquals(800, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == TsFileGeneratorUtils.getAlignDeviceOffset()) {
          assertEquals(800, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different aligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0.s0, d0.s1, d2.s4 and d3.s4 is deleted in each file.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithAllDataDeletedInTimeseries()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, true);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, true, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, true, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, true, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, true, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    seriesPaths.add(
        COMPACTION_TEST_SG
            + PATH_SEPARATOR
            + "d"
            + TsFileGeneratorUtils.getAlignDeviceOffset()
            + PATH_SEPARATOR
            + "s0");
    seriesPaths.add(
        COMPACTION_TEST_SG
            + PATH_SEPARATOR
            + "d"
            + TsFileGeneratorUtils.getAlignDeviceOffset()
            + PATH_SEPARATOR
            + "s1");
    seriesPaths.add(
        COMPACTION_TEST_SG
            + PATH_SEPARATOR
            + "d"
            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)
            + PATH_SEPARATOR
            + "s4");
    seriesPaths.add(
        COMPACTION_TEST_SG
            + PATH_SEPARATOR
            + "d"
            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
            + PATH_SEPARATOR
            + "s4");
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 0)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j == 4)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j == 4)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == TsFileGeneratorUtils.getAlignDeviceOffset()) {
          assertEquals(800, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 0)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() && j == 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j == 4)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j == 4)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if (i == TsFileGeneratorUtils.getAlignDeviceOffset()) {
          assertEquals(800, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different aligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0, d1 and d2 is deleted in each file. The first target file is empty.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithAllDataDeletedInOneTargetFile()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, true);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, true, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, true, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, true, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, true, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + TsFileGeneratorUtils.getAlignDeviceOffset()
              + PATH_SEPARATOR
              + "s"
              + i);
      seriesPaths.add(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1)
              + PATH_SEPARATOR
              + "s"
              + i);
      seriesPaths.add(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)
              + PATH_SEPARATOR
              + "s"
              + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        }
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset())
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        }
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset())
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different aligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   *
   * <p>UnSeq files<br>
   * first, second and third file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219, 250 ~ 449 and 480
   * ~ 679, value range is 10020 ~ 10219, 10250 ~ 10449 and 10480 ~ 10679.<br>
   * forth and fifth file has d0 and s0 ~ s4, time range is 450 ~ 549 and 550 ~ 649, value range is
   * 20450 ~ 20549 and 20550 ~ 20649.
   *
   * <p>The data of d0, d1 and d2 is deleted in each file. The first target file is empty.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithFileTimeIndexResource()
      throws IOException, WriteProcessException, MetadataException, StorageEngineException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, true);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, true, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, true, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, true, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, true, false);

    // generate mods file
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      seriesPaths.add(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + TsFileGeneratorUtils.getAlignDeviceOffset()
              + PATH_SEPARATOR
              + "s"
              + i);
      seriesPaths.add(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1)
              + PATH_SEPARATOR
              + "s"
              + i);
      seriesPaths.add(
          COMPACTION_TEST_SG
              + PATH_SEPARATOR
              + "d"
              + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)
              + PATH_SEPARATOR
              + "s"
              + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    for (TsFileResource resource : seqResources) {
      resource.setTimeIndexType((byte) 0);
    }
    for (TsFileResource resource : unseqResources) {
      resource.setTimeIndexType((byte) 0);
    }

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                seqResources,
                unseqResources,
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        }
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset())
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    CompactionUtils.compact(seqResources, unseqResources, targetResources, COMPACTION_TEST_SG);
    CompactionUtils.moveToTargetFile(targetResources, false, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        AlignedPath path =
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                Collections.singletonList("s" + j),
                schemas);
        IBatchReader tsFilesReader =
            new SeriesRawDataBatchReader(
                path,
                TSDataType.VECTOR,
                EnvironmentUtils.TEST_QUERY_CONTEXT,
                targetResources,
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= batchData.currentTime() && batchData.currentTime() < 550)
                    || (550 <= batchData.currentTime() && batchData.currentTime() < 650))) {
              assertEquals(
                  batchData.currentTime() + 20000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= batchData.currentTime() && batchData.currentTime() < 220)
                    || (250 <= batchData.currentTime() && batchData.currentTime() < 450)
                    || (480 <= batchData.currentTime() && batchData.currentTime() < 680))) {
              assertEquals(
                  batchData.currentTime() + 10000,
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  batchData.currentTime(),
                  ((TsPrimitiveType[]) (batchData.currentValue()))[0].getValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        }
        if ((i == TsFileGeneratorUtils.getAlignDeviceOffset())
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1)
            || (i == TsFileGeneratorUtils.getAlignDeviceOffset() + 2)) {
          assertEquals(0, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1280, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j < 4) {
          assertEquals(1230, count);
        } else if ((i == TsFileGeneratorUtils.getAlignDeviceOffset() + 1 && j == 4)) {
          assertEquals(600, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  private void generateModsFile(
      List<String> seriesPaths, List<TsFileResource> resources, long startValue, long endValue)
      throws IllegalPathException, IOException {
    for (TsFileResource resource : resources) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (String path : seriesPaths) {
        deleteMap.put(path, new Pair<>(startValue, endValue));
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, false);
    }
  }
}
