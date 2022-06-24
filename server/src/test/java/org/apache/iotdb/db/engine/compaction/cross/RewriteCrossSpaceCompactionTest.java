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
package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileName;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.conf.IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class RewriteCrossSpaceCompactionTest extends AbstractCompactionTest {

  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(10480);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
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
  public void testAlignedCrossSpaceCompactionWithAllDataDeletedInTimeseries() throws Exception {
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
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE, false);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE, false);
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);

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

    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.call();
    Assert.assertEquals(10, tsFileManager.getTsFileList(true).size());

    for (TsFileResource resource : seqResources) {
      resource.resetModFile();
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource seqResource : tsFileManager.getTsFileList(true)) {
      seqResource.resetModFile();
      Assert.assertTrue(seqResource.getModFile().exists());
      Assert.assertEquals(24, seqResource.getModFile().getModifications().size());
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
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
                tsFileManager.getTsFileList(true),
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
  public void testAlignedCrossSpaceCompactionWithAllDataDeletedInOneTargetFile() throws Exception {
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
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE, false);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE, false);
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE, true);

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
    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.call();
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).size());

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getModFile().exists());
    }
    for (TsFileResource resource : targetResources) {
      resource.setFile(
          new File(
              resource
                  .getTsFilePath()
                  .replace(CROSS_COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX)));
      if (!resource.getTsFile().exists()) {
        continue;
      }
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(180, resource.getModFile().getModifications().size());
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

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
                tsFileManager.getTsFileList(true),
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
   * <p>The data of d3.s0 is deleted in each file. Test when there is a deletion to the file before
   * compaction, then comes to a deletion during compaction.
   */
  @Test
  public void testOneDeletionDuringCompaction() throws Exception {
    DataRegion vsgp =
        new DataRegion(
            STORAGE_GROUP_DIR.getPath(),
            "0",
            new TsFileFlushPolicy.DirectFlushPolicy(),
            COMPACTION_TEST_SG);
    registerTimeseriesInMManger(4, 5, true);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, true, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, true, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, true, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, true, false);
    vsgp.getTsFileResourceManager().addAll(seqResources, true);
    vsgp.getTsFileResourceManager().addAll(unseqResources, false);
    vsgp.delete(
        new PartialPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        0,
        1000,
        0,
        null);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            vsgp.getTsFileResourceManager(),
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.setSourceFilesToCompactionCandidate();
    task.checkValidAndSetMerging();
    // delete data in source file during compaction
    vsgp.delete(
        new PartialPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        0,
        1200,
        0,
        null);
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource resource = seqResources.get(i);
      resource.resetModFile();
      Assert.assertTrue(resource.getCompactionModFile().exists());
      Assert.assertEquals(1, resource.getCompactionModFile().getModifications().size());
      Assert.assertTrue(resource.getModFile().exists());
      if (i == 3) {
        Assert.assertEquals(1, resource.getModFile().getModifications().size());
      } else {
        Assert.assertEquals(2, resource.getModFile().getModifications().size());
      }
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.getCompactionModFile().exists());
      Assert.assertEquals(1, resource.getCompactionModFile().getModifications().size());
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(2, resource.getModFile().getModifications().size());
    }
    task.call();
    Assert.assertEquals(11, vsgp.getTsFileManager().getTsFileList(true).size());
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource seqResource : vsgp.getTsFileManager().getTsFileList(true)) {
      Assert.assertTrue(seqResource.getModFile().exists());
      Assert.assertEquals(6, seqResource.getModFile().getModifications().size());
      Assert.assertFalse(seqResource.getCompactionModFile().exists());
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
   * <p>The data of d3.s0 is deleted in each file. Test when there is a deletion to the file before
   * compaction, then comes to serveral deletions during compaction.
   */
  @Test
  public void testSeveralDeletionsDuringCompaction() throws Exception {
    DataRegion vsgp =
        new DataRegion(
            STORAGE_GROUP_DIR.getPath(),
            "0",
            new TsFileFlushPolicy.DirectFlushPolicy(),
            COMPACTION_TEST_SG);
    registerTimeseriesInMManger(4, 5, true);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, true, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, true, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, true, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, true, false);
    vsgp.getTsFileResourceManager().addAll(seqResources, true);
    vsgp.getTsFileResourceManager().addAll(unseqResources, false);
    vsgp.delete(
        new PartialPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        0,
        1000,
        0,
        null);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            vsgp.getTsFileResourceManager(),
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.setSourceFilesToCompactionCandidate();
    task.checkValidAndSetMerging();
    // delete data in source file during compaction
    vsgp.delete(
        new PartialPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        0,
        1200,
        0,
        null);
    vsgp.delete(
        new PartialPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        0,
        1800,
        0,
        null);
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource resource = seqResources.get(i);
      resource.resetModFile();
      Assert.assertTrue(resource.getCompactionModFile().exists());
      Assert.assertEquals(2, resource.getCompactionModFile().getModifications().size());
      Assert.assertTrue(resource.getModFile().exists());
      if (i == 3) {
        Assert.assertEquals(2, resource.getModFile().getModifications().size());
      } else {
        Assert.assertEquals(3, resource.getModFile().getModifications().size());
      }
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.getCompactionModFile().exists());
      Assert.assertEquals(2, resource.getCompactionModFile().getModifications().size());
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(3, resource.getModFile().getModifications().size());
    }
    task.call();
    Assert.assertEquals(11, vsgp.getTsFileManager().getTsFileList(true).size());
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource seqResource : vsgp.getTsFileManager().getTsFileList(true)) {
      Assert.assertTrue(seqResource.getModFile().exists());
      Assert.assertEquals(12, seqResource.getModFile().getModifications().size());
      Assert.assertFalse(seqResource.getCompactionModFile().exists());
    }
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
  public void testAlignedCrossSpaceCompactionWithDifferentTimeseriesAndSplitLargeFiles()
      throws Exception {
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

    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.call();

    Assert.assertEquals(12, tsFileManager.getTsFileList(true).size());

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
                tsFileManager.getTsFileList(true),
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
   * Total 5 seq files and 3 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   * fifth file has d0 and s0, time range and value range is 1350 ~ 1849.<br>
   *
   * <p>UnSeq files<br>
   * first and second file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219 and 250 ~ 299, value
   * range is 10020 ~ 10219 and 10250 ~ 10299.<br>
   * third file has d0 ~ d3 and s0 ~ s4, time range is 650 ~ 1349 and value range is 10650 ~ 11349.
   *
   * <p>Therefore, 3 unseq files only overlap with seq files 1,3,4, but not seq file 2.
   */
  @Test
  public void testCrossSpaceCompactionWithDifferentTimeseriesAndSplitLargeFiles() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    long oldTargetFileSize =
        IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(10480);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(1, 1, 1, 500, 1350, 1350, 0, 0, false, true);
    createFiles(1, 3, 4, 200, 20, 10020, 0, 0, false, false);
    createFiles(1, 3, 4, 50, 250, 10250, 0, 0, false, false);
    createFiles(1, 4, 5, 700, 650, 10650, 0, 0, false, false);

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
            if (i < 2) {
              if (batchData.currentTime() < 20
                  || (batchData.currentTime() > 219 && batchData.currentTime() < 250)
                  || (batchData.currentTime() > 300 && batchData.currentTime() < 650)
                  || batchData.currentTime() > 1349) {
                assertEquals(batchData.currentTime(), batchData.currentValue());
              } else {
                assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
              }
            } else {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 && j == 0) {
          assertEquals(1800, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1300, count);
        } else if (i < 3 && j < 4) {
          assertEquals(950, count);
        } else {
          assertEquals(700, count);
        }
      }
    }

    // rename seq files with the same timestamp and different versions
    for (int i = 1; i <= seqResources.size(); i++) {
      File resourceFile =
          new File(seqResources.get(i - 1).getTsFile() + TsFileResource.RESOURCE_SUFFIX);
      resourceFile.renameTo(
          new File(
              resourceFile.getParentFile(),
              "2-" + i + "-0-0.tsfile" + TsFileResource.RESOURCE_SUFFIX));
      File newTsFile =
          new File(seqResources.get(0).getTsFile().getParentFile(), "2-" + i + "-0-0.tsfile");
      seqResources.get(i - 1).getTsFile().renameTo(newTsFile);
      seqResources.get(i - 1).setFile(newTsFile);
      seqResources.get(i - 1).setVersion(i);
    }

    TsFileManager tsFileManager =
        new TsFileManager(COMPACTION_TEST_SG, "0", STORAGE_GROUP_DIR.getPath());
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    List<TsFileResource> sourceSeqFiles = new ArrayList<>();
    sourceSeqFiles.add(seqResources.get(0));
    sourceSeqFiles.add(seqResources.get(2));
    sourceSeqFiles.add(seqResources.get(3));
    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            sourceSeqFiles,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.call();
    List<TsFileResource> seqFiles = tsFileManager.getTsFileList(true);
    Assert.assertEquals(12, tsFileManager.getTsFileList(true).size());

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
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
                tsFileManager.getTsFileList(true),
                new ArrayList<>(),
                null,
                null,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          while (batchData.hasCurrent()) {
            if (i < 2) {
              if (batchData.currentTime() < 20
                  || (batchData.currentTime() > 219 && batchData.currentTime() < 250)
                  || (batchData.currentTime() > 300 && batchData.currentTime() < 650)
                  || batchData.currentTime() > 1349) {
                assertEquals(batchData.currentTime(), batchData.currentValue());
              } else {
                assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
              }
            } else {
              assertEquals(batchData.currentTime() + 10000, batchData.currentValue());
            }
            count++;
            batchData.next();
          }
        }
        tsFilesReader.close();
        if (i == 0 && j == 0) {
          assertEquals(1800, count);
        } else if (i < 2 && j < 3) {
          assertEquals(1300, count);
        } else if (i < 3 && j < 4) {
          assertEquals(950, count);
        } else {
          assertEquals(700, count);
        }
      }
    }
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(oldTargetFileSize);
  }

  /**
   * Total 5 seq files and 3 unseq files, each file has different nonAligned timeseries.
   *
   * <p>Seq files<br>
   * first and second file has d0 ~ d1 and s0 ~ s2, time range is 0 ~ 299 and 350 ~ 649, value range
   * is 0 ~ 299 and 350 ~ 649.<br>
   * third and forth file has d0 ~ d3 and s0 ~ S4,time range is 700 ~ 999 and 1050 ~ 1349, value
   * range is 700 ~ 999 and 1050 ~ 1349.<br>
   * fifth file has d0 and s0, time range and value range is 1350 ~ 1849.<br>
   *
   * <p>UnSeq files<br>
   * first and second file has d0 ~ d2 and s0 ~ s3, time range is 20 ~ 219 and 250 ~ 299, value
   * range is 10020 ~ 10219 and 10250 ~ 10299.<br>
   * third file has d0 ~ d3 and s0 ~ s4, time range is 650 ~ 1349 and value range is 10650 ~ 11349.
   *
   * <p>Therefore, 3 unseq files only overlap with seq files 1,3,4, but not seq file 2.
   *
   * <p>The data of d3.s0 is deleted in each file. Test when there is a deletion to the file before
   * * compaction, then comes to serveral deletions during compaction.
   */
  @Test
  public void testCrossSpaceCompactionWithDeletionDuringCompactionAndSplitLargeFiles()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    long oldTargetFileSize =
        IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(10480);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(1, 1, 1, 500, 1350, 1350, 0, 0, false, true);
    createFiles(1, 3, 4, 200, 20, 10020, 0, 0, false, false);
    createFiles(1, 3, 4, 50, 250, 10250, 0, 0, false, false);
    createFiles(1, 4, 5, 700, 650, 10650, 0, 0, false, false);

    // rename seq files with the same timestamp and different versions
    for (int i = 1; i <= seqResources.size(); i++) {
      File resourceFile =
          new File(seqResources.get(i - 1).getTsFile() + TsFileResource.RESOURCE_SUFFIX);
      resourceFile.renameTo(
          new File(
              resourceFile.getParentFile(),
              "2-" + i + "-0-0.tsfile" + TsFileResource.RESOURCE_SUFFIX));
      File newTsFile =
          new File(seqResources.get(0).getTsFile().getParentFile(), "2-" + i + "-0-0.tsfile");
      seqResources.get(i - 1).getTsFile().renameTo(newTsFile);
      seqResources.get(i - 1).setFile(newTsFile);
      seqResources.get(i - 1).setVersion(i);
    }

    DataRegion vsgp =
        new DataRegion(
            STORAGE_GROUP_DIR.getPath(),
            "0",
            new TsFileFlushPolicy.DirectFlushPolicy(),
            COMPACTION_TEST_SG);
    vsgp.getTsFileResourceManager().addAll(seqResources, true);
    vsgp.getTsFileResourceManager().addAll(unseqResources, false);
    // delete data in source files before compaction
    vsgp.delete(
        new PartialPath(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3" + PATH_SEPARATOR + "s0"),
        0,
        1000,
        0,
        null);

    List<TsFileResource> sourceSeqFiles = new ArrayList<>();
    sourceSeqFiles.add(seqResources.get(0));
    sourceSeqFiles.add(seqResources.get(2));
    sourceSeqFiles.add(seqResources.get(3));

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            vsgp.getTsFileResourceManager(),
            sourceSeqFiles,
            unseqResources,
            new ReadPointCompactionPerformer(),
            new AtomicInteger(0));
    task.setSourceFilesToCompactionCandidate();
    task.checkValidAndSetMerging();
    // delete data in source files during compaction
    vsgp.delete(
        new PartialPath(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3" + PATH_SEPARATOR + "s0"),
        0,
        1200,
        0,
        null);
    vsgp.delete(
        new PartialPath(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3" + PATH_SEPARATOR + "s0"),
        0,
        1800,
        0,
        null);
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource resource = seqResources.get(i);
      resource.resetModFile();
      if (i == 0 || i == 2 || i == 3) {
        Assert.assertTrue(resource.getCompactionModFile().exists());
        Assert.assertEquals(2, resource.getCompactionModFile().getModifications().size());
      } else {
        Assert.assertFalse(resource.getCompactionModFile().exists());
      }
      Assert.assertTrue(resource.getModFile().exists());
      if (i == 3) {
        Assert.assertEquals(2, resource.getModFile().getModifications().size());
      } else {
        Assert.assertEquals(3, resource.getModFile().getModifications().size());
      }
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertTrue(resource.getCompactionModFile().exists());
      Assert.assertEquals(2, resource.getCompactionModFile().getModifications().size());
      Assert.assertTrue(resource.getModFile().exists());
      Assert.assertEquals(3, resource.getModFile().getModifications().size());
    }
    task.call();
    Assert.assertEquals(12, vsgp.getTsFileManager().getTsFileList(true).size());
    for (TsFileResource resource : sourceSeqFiles) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.getModFile().exists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource seqResource : vsgp.getTsFileManager().getTsFileList(true)) {
      Assert.assertTrue(seqResource.getModFile().exists());
      Assert.assertFalse(seqResource.getCompactionModFile().exists());
      if (TsFileName.parseCrossCompactionCnt(seqResource.getTsFile().getName()) == 0) {
        Assert.assertEquals(3, seqResource.getModFile().getModifications().size());
      }
    }
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(oldTargetFileSize);
  }

  private void generateModsFile(
      List<String> seriesPaths,
      List<TsFileResource> resources,
      long startValue,
      long endValue,
      boolean isCompactionMods)
      throws IllegalPathException, IOException {
    for (TsFileResource resource : resources) {
      Map<String, Pair<Long, Long>> deleteMap = new HashMap<>();
      for (String path : seriesPaths) {
        deleteMap.put(path, new Pair<>(startValue, endValue));
      }
      CompactionFileGeneratorUtils.generateMods(deleteMap, resource, isCompactionMods);
    }
  }
}
