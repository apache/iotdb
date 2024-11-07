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
package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
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

import static org.apache.iotdb.commons.conf.IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class RewriteCrossSpaceCompactionWithReadPointPerformerTest extends AbstractCompactionTest {

  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    WALRecoverManager.getInstance().setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(1));
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
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
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            long currentValue = batchData.getColumn(0).getLong(readIndex);
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= currentTime && currentTime < 550)
                    || (550 <= currentTime && currentTime < 650))) {
              assertEquals(currentTime + 20000, currentValue);
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= currentTime && currentTime < 220)
                    || (250 <= currentTime && currentTime < 450)
                    || (480 <= currentTime && currentTime < 680))) {
              assertEquals(currentTime + 10000, currentValue);
            } else {
              assertEquals(currentTime, currentValue);
            }
            count++;
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
            0,
            0);
    task.start();

    for (TsFileResource resource : seqResources) {
      resource.resetModFile();
      Assert.assertFalse(resource.anyModFileExists());
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertFalse(resource.anyModFileExists());
    }
    for (TsFileResource resource : targetResources) {
      resource.setFile(
          new File(
              resource
                  .getTsFilePath()
                  .replace(CROSS_COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX)));
      resource.resetModFile();
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertEquals(4, resource.getAllModEntries().size());
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            long currentValue = batchData.getColumn(0).getLong(readIndex);
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= currentTime && currentTime < 550)
                    || (550 <= currentTime && currentTime < 650))) {
              assertEquals(currentTime + 20000, currentValue);
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= currentTime && currentTime < 220)
                    || (250 <= currentTime && currentTime < 450)
                    || (480 <= currentTime && currentTime < 680))) {
              assertEquals(currentTime + 10000, currentValue);
            } else {
              assertEquals(currentTime, currentValue);
            }
            count++;
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
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            long currentValue = batchData.getColumn(0).getLong(readIndex);
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= currentTime && currentTime < 550)
                    || (550 <= currentTime && currentTime < 650))) {
              assertEquals(currentTime + 20000, currentValue);
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= currentTime && currentTime < 220)
                    || (250 <= currentTime && currentTime < 450)
                    || (480 <= currentTime && currentTime < 680))) {
              assertEquals(currentTime + 10000, currentValue);
            } else {
              assertEquals(currentTime, currentValue);
            }
            count++;
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
            0,
            0);
    task.start();

    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.anyModFileExists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.anyModFileExists());
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
      Assert.assertTrue(resource.anyModFileExists());
      Assert.assertEquals(30, resource.getAllModEntries().size());
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 4;
        i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            long currentValue = batchData.getColumn(0).getLong(readIndex);
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= currentTime && currentTime < 550)
                    || (550 <= currentTime && currentTime < 650))) {
              assertEquals(currentTime + 20000, currentValue);
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= currentTime && currentTime < 220)
                    || (250 <= currentTime && currentTime < 450)
                    || (480 <= currentTime && currentTime < 680))) {
              assertEquals(currentTime + 10000, currentValue);
            } else {
              assertEquals(currentTime, currentValue);
            }
            count++;
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
   * <p>The data of d3.s0 is deleted. Test when there is a deletion to the file before compaction,
   * then comes to a deletion during compaction.
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
    MeasurementPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 0, 1000);
    deleteDataNode.setSearchIndex(0);
    vsgp.deleteByDevice(
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        deleteDataNode);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            vsgp.getTsFileResourceManager(),
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            0,
            0);
    task.setSourceFilesToCompactionCandidate();
    seqResources.forEach(f -> f.setStatus(TsFileResourceStatus.COMPACTING));
    unseqResources.forEach(f -> f.setStatus(TsFileResourceStatus.COMPACTING));
    // delete data in source file during compaction
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path), 0, 1200);
    deleteDataNode2.setSearchIndex(0);
    vsgp.deleteByDevice(
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        deleteDataNode2);
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource resource = seqResources.get(i);
      resource.resetModFile();
      if (i < 2) {
        Assert.assertFalse(resource.getCompactionModFile().exists());
        Assert.assertFalse(resource.anyModFileExists());
      } else if (i == 2) {
        Assert.assertTrue(resource.getCompactionModFile().exists());
        Assert.assertTrue(resource.anyModFileExists());
        Assert.assertEquals(2, resource.getAllModEntries().size());
        Assert.assertEquals(1, resource.getCompactionModFile().getAllMods().size());
      } else {
        Assert.assertTrue(resource.getCompactionModFile().exists());
        Assert.assertTrue(resource.anyModFileExists());
        Assert.assertEquals(1, resource.getAllModEntries().size());
        Assert.assertEquals(1, resource.getCompactionModFile().getAllMods().size());
      }
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertFalse(resource.getCompactionModFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
    }
    task.start();
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource seqResource = seqResources.get(i);
      TsFileResource resource =
          new TsFileResource(
              TsFileNameGenerator.increaseCrossCompactionCnt(seqResource.getTsFile()));
      if (i < 2) {
        Assert.assertFalse(resource.getCompactionModFile().exists());
        Assert.assertFalse(resource.anyModFileExists());
      } else {
        Assert.assertFalse(resource.getCompactionModFile().exists());
        Assert.assertTrue(resource.anyModFileExists());
        Assert.assertEquals(1, resource.getAllModEntries().size());
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
   * <p>The data of d3.s0 is deleted. Test when there is a deletion to the file before compaction,
   * then comes to serveral deletions during compaction.
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

    MeasurementPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 0, 1000);
    deleteDataNode.setSearchIndex(0);
    vsgp.deleteByDevice(
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        deleteDataNode);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            vsgp.getTsFileResourceManager(),
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            0,
            0);
    task.setSourceFilesToCompactionCandidate();
    seqResources.forEach(f -> f.setStatus(TsFileResourceStatus.COMPACTING));
    unseqResources.forEach(f -> f.setStatus(TsFileResourceStatus.COMPACTING));
    // delete data in source file during compaction
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path), 0, 1200);
    deleteDataNode2.setSearchIndex(0);
    vsgp.deleteByDevice(
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        deleteDataNode2);

    DeleteDataNode deleteDataNode3 =
        new DeleteDataNode(new PlanNodeId("3"), Collections.singletonList(path), 0, 1800);
    deleteDataNode3.setSearchIndex(0);
    vsgp.deleteByDevice(
        new MeasurementPath(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)
                + PATH_SEPARATOR
                + "s0"),
        deleteDataNode3);
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource resource = seqResources.get(i);
      resource.resetModFile();
      if (i < 2) {
        Assert.assertFalse(resource.getCompactionModFile().exists());
        Assert.assertFalse(resource.anyModFileExists());
      } else if (i == 2) {
        Assert.assertTrue(resource.getCompactionModFile().exists());
        Assert.assertTrue(resource.anyModFileExists());
        Assert.assertEquals(3, resource.getAllModEntries().size());
        Assert.assertEquals(2, resource.getCompactionModFile().getAllMods().size());
      } else {
        Assert.assertTrue(resource.getCompactionModFile().exists());
        Assert.assertTrue(resource.anyModFileExists());
        Assert.assertEquals(2, resource.getAllModEntries().size());
        Assert.assertEquals(2, resource.getCompactionModFile().getAllMods().size());
      }
    }
    for (TsFileResource resource : unseqResources) {
      resource.resetModFile();
      Assert.assertFalse(resource.getCompactionModFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
    }
    task.start();
    for (TsFileResource resource : seqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (TsFileResource resource : unseqResources) {
      Assert.assertFalse(resource.getTsFile().exists());
      Assert.assertFalse(resource.anyModFileExists());
      Assert.assertFalse(resource.getCompactionModFile().exists());
    }
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource seqResource = seqResources.get(i);
      TsFileResource resource =
          new TsFileResource(
              TsFileNameGenerator.increaseCrossCompactionCnt(seqResource.getTsFile()));
      if (i < 2) {
        Assert.assertFalse(resource.getCompactionModFile().exists());
        Assert.assertFalse(resource.anyModFileExists());
      } else {
        Assert.assertFalse(resource.getCompactionModFile().exists());
        Assert.assertTrue(resource.anyModFileExists());
        Assert.assertEquals(2, resource.getAllModEntries().size());
      }
    }
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
