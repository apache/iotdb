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
package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskQueue;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionWorker;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.tools.validate.TsFileValidationTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.IBatchDataIterator;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
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

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;

public class FastCrossCompactionPerformerTest extends AbstractCompactionTest {
  private List<TsFileResource> targetResources = new ArrayList<>();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    validateSeqFiles();
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    targetResources.clear();
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
  public void testCrossSpaceCompactionWithSameTimeseries() throws Exception {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    IFullPath path =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"),
            new MeasurementSchema("s1", TSDataType.INT64));
    IDataBlockReader tsBlockReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            seqResources,
            unseqResources,
            true);
    int count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
      while (iterator.hasNext()) {
        if (iterator.currentTime() % 100 < 50) {
          assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
        } else {
          assertEquals(iterator.currentTime(), iterator.currentValue());
        }

        count++;
        iterator.next();
      }
    }
    tsBlockReader.close();
    assertEquals(500, count);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    tsBlockReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            targetResources,
            new ArrayList<>(),
            true);
    count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
      while (iterator.hasNext()) {
        if (iterator.currentTime() % 100 < 50) {
          assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
        } else {
          assertEquals(iterator.currentTime(), iterator.currentValue());
        }

        count++;
        iterator.next();
      }
    }
    tsBlockReader.close();
    assertEquals(500, count);
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
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
  public void testCrossSpaceCompactionWithDifferentTimeseries() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
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
  public void testCrossSpaceCompactionWithAllDataDeletedInTimeseries() throws Exception {
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
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
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
  public void testCrossSpaceCompactionWithAllDataDeletedInDevice() throws Exception {
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
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 2; i < 4; i++) {
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
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
   * <p>The data of d0, d1 and d2 is deleted in each file. Data in the first and second target file
   * is all deleted.
   */
  @Test
  public void testCrossSpaceCompactionWithAllDataDeletedInOneTargetFile() throws Exception {
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
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    targetResources.removeIf(x -> x.isDeleted());
    Assert.assertEquals(2, targetResources.size());
    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 0; i < 2; i++) {
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 1 || i == 2) {
          assertEquals(0, count);
        } else {
          assertEquals(600, count);
        }
      }
    }
  }

  /**
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
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
      throws IOException, WriteProcessException, MetadataException, Exception {
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
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    Assert.assertEquals(4, targetResources.size());
    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 2; i < 3; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.clear();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 3; i < 4; i++) {
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

  @Test
  public void testCrossSpaceCompactionWithNewDeviceInUnseqFile() {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    try {
      registerTimeseriesInMManger(6, 6, false);
      createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
      createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
      createFiles(3, 6, 6, 200, 20, 10020, 30, 30, false, false);
      createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

      List<TsFileResource> targetResources =
          CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
      ICompactionPerformer performer =
          new FastCompactionPerformer(seqResources, unseqResources, targetResources);
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
      Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
      CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

      Assert.assertEquals(4, targetResources.size());
      List<IDeviceID> deviceIdList = new ArrayList<>();
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      for (int i = 0; i < 2; i++) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        check(targetResources.get(i), deviceIdList);
      }
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      for (int i = 2; i < 3; i++) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        check(targetResources.get(i), deviceIdList);
      }
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d4"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d5"));
      for (int i = 3; i < 4; i++) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d5")));
        check(targetResources.get(i), deviceIdList);
      }
    } catch (MetadataException
        | IOException
        | WriteProcessException
        | StorageEngineException
        | InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCrossSpaceCompactionWithDeviceMaxTimeLaterInUnseqFile() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    try {
      registerTimeseriesInMManger(6, 6, false);
      createFiles(2, 2, 3, 200, 0, 0, 0, 0, false, true);
      createFiles(3, 4, 4, 300, 20, 10020, 0, 0, false, false);

      List<TsFileResource> targetResources =
          CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
      ICompactionPerformer performer =
          new FastCompactionPerformer(seqResources, unseqResources, targetResources);
      performer.setSummary(new FastCompactionTaskSummary());
      performer.perform();
      Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
      Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
      CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

      Assert.assertEquals(2, targetResources.size());
      List<IDeviceID> deviceIdList = new ArrayList<>();
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
      for (int i = 0; i < 1; i++) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        check(targetResources.get(i), deviceIdList);
      }
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
      deviceIdList.add(
          IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
      for (int i = 1; i < 2; i++) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        check(targetResources.get(i), deviceIdList);
      }

      for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 4; j++) {
          IFullPath path =
              new NonAlignedFullPath(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                  new MeasurementSchema("s" + j, TSDataType.INT64));
          IDataBlockReader tsBlockReader =
              new SeriesDataBlockReader(
                  path,
                  FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                      EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                  targetResources,
                  new ArrayList<>(),
                  true);
          int count = 0;
          while (tsBlockReader.hasNextBatch()) {
            TsBlock block = tsBlockReader.nextBatch();
            IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
            while (iterator.hasNext()) {
              if (iterator.currentTime() < 20) {
                assertEquals(iterator.currentTime(), iterator.currentValue());
              } else {
                assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
              }
              count++;
              iterator.next();
            }
          }
          tsBlockReader.close();
          if (i < 2 && j < 3) {
            assertEquals(920, count);
          } else {
            assertEquals(900, count);
          }
        }
      }
    } catch (MetadataException
        | IOException
        | WriteProcessException
        | StorageEngineException
        | InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
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
  public void testAlignedCrossSpaceCompactionWithSameTimeseries() throws Exception {
    registerTimeseriesInMManger(2, 3, true);
    createFiles(5, 2, 3, 100, 0, 0, 0, 0, true, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, true, false);

    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s1", TSDataType.INT64));
    IFullPath path =
        new AlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000"),
            Collections.singletonList("s1"),
            schemas);
    IDataBlockReader tsBlockReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            seqResources,
            unseqResources,
            true);
    int count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
      while (iterator.hasNext()) {
        if (iterator.currentTime() % 100 < 50) {
          assertEquals(
              iterator.currentTime() + 10000,
              ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
        } else {
          assertEquals(
              iterator.currentTime(),
              ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
        }
        count++;
        iterator.next();
      }
    }
    tsBlockReader.close();
    assertEquals(500, count);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    tsBlockReader =
        new SeriesDataBlockReader(
            path,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            targetResources,
            new ArrayList<>(),
            true);
    count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
      while (iterator.hasNext()) {
        if (iterator.currentTime() % 100 < 50) {
          assertEquals(
              iterator.currentTime() + 10000,
              ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
        } else {
          assertEquals(
              iterator.currentTime(),
              ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
        }

        count++;
        iterator.next();
      }
    }
    tsBlockReader.close();
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
  public void testAlignedCrossSpaceCompactionWithDifferentTimeseries() throws Exception {
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
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    deleteTimeseriesInMManager(seriesPaths);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    Assert.assertEquals(4, targetResources.size());
    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10001"));
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10001")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10002")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10002"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003"));
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10001")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10002")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003")));
      check(targetResources.get(i), deviceIdList);
    }

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    deleteTimeseriesInMManager(seriesPaths);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(resource -> resource == null);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
   * Different source files have timeseries with the same path, but different data types. Because
   * timeseries in the former file is been deleted.
   */
  @Test
  public void testCrossSpaceCompactionWithSameTimeseriesInDifferentSourceFiles() throws Exception {
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
    deleteTimeseriesInMManager(seriesPaths);
    setDataType(TSDataType.TEXT);
    registerTimeseriesInMManger(2, 7, false);
    List<Integer> deviceIndex = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      deviceIndex.add(i);
    }
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      measurementIndex.add(i);
    }

    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1350, 0, false, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(x -> x.isDeleted());
    Assert.assertEquals(2, targetResources.size());

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 0; i < 2; i++) {
      if (i == 0) {
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      TSDataType tsDataType = i < 2 ? TSDataType.TEXT : TSDataType.INT64;
      for (int j = 0; j < 7; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, tsDataType));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 2 && j < 7) {
          assertEquals(300, count);
        } else if (i == 3 && j < 5) {
          assertEquals(600, count);
        } else {
          assertEquals(0, count);
        }
      }
    }
  }

  /** Each source file has different device. */
  @Test
  public void testCrossSpaceCompactionWithDifferentDevicesInDifferentSourceFiles()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, false);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      measurementIndex.add(i);
    }

    deviceIndex.add(0);
    deviceIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, false, true);

    deviceIndex.clear();
    deviceIndex.add(1);
    deviceIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, false, true);

    deviceIndex.clear();
    deviceIndex.add(2);
    deviceIndex.add(4);
    deviceIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, false, true);

    deviceIndex.clear();
    deviceIndex.add(1);
    deviceIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, false, false);

    deviceIndex.clear();
    deviceIndex.add(1);
    deviceIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 400, 600, 0, false, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d4"));
    for (int i = 0; i < 3; i++) {
      if (i == 0) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
      } else if (i == 1) {
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.TEXT));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 2 || i == 3) {
          assertEquals(600, count);
        } else if (i == 1) {
          assertEquals(800, count);
        } else {
          assertEquals(500, count);
        }
      }
    }
  }

  /** Each source file has same device with different measurements. */
  @Test
  public void testCrossSpaceCompactionWithDifferentMeasurementsInDifferentSourceFiles()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 5, false);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      deviceIndex.add(i);
    }

    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, false, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, false, true);

    measurementIndex.clear();
    measurementIndex.add(2);
    measurementIndex.add(4);
    measurementIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, false, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, false, false);

    measurementIndex.clear();
    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 400, 0, false, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d4"));
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.TEXT));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (j == 0 || j == 2) {
          assertEquals(800, count);
        } else if (j == 1 || j == 4) {
          assertEquals(500, count);
        } else {
          assertEquals(300, count);
        }
      }
    }
  }

  /** Each source file has different devices and different measurements. */
  @Test
  public void testCrossSpaceCompactionWithDifferentDevicesAndMeasurementsInDifferentSourceFiles()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    deviceIndex.add(0);
    deviceIndex.add(1);

    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, false, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, false, true);

    deviceIndex.add(2);
    deviceIndex.add(3);
    measurementIndex.clear();
    measurementIndex.add(2);
    measurementIndex.add(4);
    measurementIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, false, true);
    deviceIndex.remove(2);
    deviceIndex.remove(2);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, false, false);

    measurementIndex.clear();
    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 600, 0, false, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 0; i < 3; i++) {
      if (i < 2) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.TEXT));
        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    TEST_QUERY_JOB_ID),
                targetResources,
                new ArrayList<>(),
                true);

        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= currentTime) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                currentTime);
            count++;
          }
        }
        tsFilesReader.close();
        if (i < 2) {
          if (j == 0 || j == 2) {
            assertEquals(800, count);
          } else if (j == 1 || j == 4) {
            assertEquals(500, count);
          } else {
            assertEquals(300, count);
          }
        } else {
          if (j == 0 || j == 2 || j == 4) {
            assertEquals(300, count);
          } else {
            assertEquals(0, count);
          }
        }
      }
    }
  }

  /** Each source file has different devices and different measurements. */
  @Test
  public void testCrossSpaceCompactionWithDifferentDevicesAndMeasurementsInDifferentSourceFiles2()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    deviceIndex.add(0);
    deviceIndex.add(1);

    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, false, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, false, true);

    deviceIndex.add(2);
    deviceIndex.add(3);
    measurementIndex.clear();
    measurementIndex.add(2);
    measurementIndex.add(4);
    measurementIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, false, true);
    deviceIndex.remove(2);
    deviceIndex.remove(2);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, false, false);

    deviceIndex.remove(0);
    measurementIndex.clear();
    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 600, 0, false, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 0; i < 3; i++) {
      if (i < 2) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.TEXT));

        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    TEST_QUERY_JOB_ID),
                targetResources,
                new ArrayList<>(),
                true);

        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= currentTime) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                currentTime);
            count++;
          }
        }
        tsFilesReader.close();
        if (i < 2) {
          if (j == 0 || j == 2) {
            if (i == 0) {
              assertEquals(600, count);
            } else {
              assertEquals(800, count);
            }
          } else if (j == 1 || j == 4) {
            assertEquals(500, count);
          } else {
            assertEquals(300, count);
          }
        } else {
          if (j == 0 || j == 2 || j == 4) {
            assertEquals(300, count);
          } else {
            assertEquals(0, count);
          }
        }
      }
    }
  }

  /**
   * Different source files have timeseries with the same path, but different data types. Because
   * timeseries in the former file is been deleted.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithSameTimeseriesInDifferentSourceFiles()
      throws Exception {
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
    deleteTimeseriesInMManager(seriesPaths);
    setDataType(TSDataType.TEXT);
    registerTimeseriesInMManger(2, 7, true);
    List<Integer> deviceIndex = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      deviceIndex.add(i);
    }
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      measurementIndex.add(i);
    }

    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1450, 0, true, true);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1350, 0, true, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(x -> x.isDeleted());
    Assert.assertEquals(3, targetResources.size());

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset())));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)));
    for (int i = 0; i < 3; i++) {
      if (i == 0) {
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
      } else if (i == 1) {
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      TSDataType tsDataType = i < 2 ? TSDataType.TEXT : TSDataType.INT64;
      for (int j = 0; j < 7; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                + PATH_SEPARATOR
                + "s"
                + j,
            Long.MIN_VALUE);
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, tsDataType));
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                        + PATH_SEPARATOR
                        + "s"
                        + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d"
                    + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                    + PATH_SEPARATOR
                    + "s"
                    + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < 2 && j < 7) {
          assertEquals(400, count);
        } else if (i == 3 && j < 5) {
          assertEquals(600, count);
        } else {
          assertEquals(0, count);
        }
      }
    }
  }

  /** Each source file has different device. */
  @Test
  public void testAlignedCrossSpaceCompactionWithDifferentDevicesInDifferentSourceFiles()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 7, true);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      measurementIndex.add(i);
    }

    deviceIndex.add(0);
    deviceIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, true, true);

    deviceIndex.clear();
    deviceIndex.add(1);
    deviceIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, true, true);

    deviceIndex.clear();
    deviceIndex.add(2);
    deviceIndex.add(4);
    deviceIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, true, true);

    deviceIndex.clear();
    deviceIndex.add(1);
    deviceIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, true, false);

    deviceIndex.clear();
    deviceIndex.add(1);
    deviceIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 400, 600, 0, true, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + TsFileGeneratorUtils.getAlignDeviceOffset()));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 4)));
    for (int i = 0; i < 3; i++) {
      if (i == 0) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 4))));
      } else if (i == 1) {
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 4))));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 4))));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                + PATH_SEPARATOR
                + "s"
                + j,
            Long.MIN_VALUE);
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.TEXT));
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                        + PATH_SEPARATOR
                        + "s"
                        + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d"
                    + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                    + PATH_SEPARATOR
                    + "s"
                    + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 0 || i == 2 || i == 3) {
          assertEquals(600, count);
        } else if (i == 1) {
          assertEquals(800, count);
        } else {
          assertEquals(500, count);
        }
      }
    }
  }

  @Test
  public void testAlignedCrossSpaceCompactionWithDifferentMeasurementsInDifferentSourceFiles()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(5, 5, true);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      deviceIndex.add(i);
    }

    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, true, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, true, true);

    measurementIndex.clear();
    measurementIndex.add(2);
    measurementIndex.add(4);
    measurementIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, true, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, true, false);

    measurementIndex.clear();
    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 600, 0, true, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + TsFileGeneratorUtils.getAlignDeviceOffset()));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 4)));
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG
                          + PATH_SEPARATOR
                          + "d"
                          + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG
                          + PATH_SEPARATOR
                          + "d"
                          + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG
                          + PATH_SEPARATOR
                          + "d"
                          + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG
                          + PATH_SEPARATOR
                          + "d"
                          + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG
                          + PATH_SEPARATOR
                          + "d"
                          + (TsFileGeneratorUtils.getAlignDeviceOffset() + 4))));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                + PATH_SEPARATOR
                + "s"
                + j,
            Long.MIN_VALUE);
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.TEXT));
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)),
                Collections.singletonList("s" + j),
                schemas);
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                        + PATH_SEPARATOR
                        + "s"
                        + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d"
                    + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                    + PATH_SEPARATOR
                    + "s"
                    + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (j == 0 || j == 2) {
          assertEquals(800, count);
        } else if (j == 1 || j == 4) {
          assertEquals(500, count);
        } else {
          assertEquals(300, count);
        }
      }
    }
  }

  /** Each source file has different devices and different measurements. */
  @Test
  public void
      testAlignedCrossSpaceCompactionWithDifferentDevicesAndMeasurementsInDifferentSourceFiles2()
          throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, true);
    List<Integer> deviceIndex = new ArrayList<>();
    List<Integer> measurementIndex = new ArrayList<>();
    deviceIndex.add(0);
    deviceIndex.add(1);

    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 0, 0, true, true);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(3);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 400, 0, true, true);

    deviceIndex.add(2);
    deviceIndex.add(3);
    measurementIndex.clear();
    measurementIndex.add(2);
    measurementIndex.add(4);
    measurementIndex.add(0);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 800, 0, true, true);
    deviceIndex.remove(2);
    deviceIndex.remove(2);

    measurementIndex.clear();
    measurementIndex.add(1);
    measurementIndex.add(4);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 100, 0, true, false);

    deviceIndex.remove(0);
    measurementIndex.clear();
    measurementIndex.add(0);
    measurementIndex.add(2);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 600, 0, true, false);

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + TsFileGeneratorUtils.getAlignDeviceOffset()));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2)));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3)));
    for (int i = 0; i < 3; i++) {
      if (i < 2) {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
      } else {
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset()))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 1))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 2))));
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG
                            + PATH_SEPARATOR
                            + "d"
                            + (TsFileGeneratorUtils.getAlignDeviceOffset() + 3))));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG
                + PATH_SEPARATOR
                + "d"
                + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                + PATH_SEPARATOR
                + "s"
                + j,
            Long.MIN_VALUE);
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.TEXT));
        IFullPath path =
            new AlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)),
                Collections.singletonList("s" + j),
                schemas);

        IDataBlockReader tsFilesReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsFilesReader.hasNextBatch()) {
          TsBlock batchData = tsFilesReader.nextBatch();
          for (int readIndex = 0, size = batchData.getPositionCount();
              readIndex < size;
              readIndex++) {
            long currentTime = batchData.getTimeByIndex(readIndex);
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG
                        + PATH_SEPARATOR
                        + "d"
                        + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                        + PATH_SEPARATOR
                        + "s"
                        + j)
                >= currentTime) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d"
                    + (TsFileGeneratorUtils.getAlignDeviceOffset() + i)
                    + PATH_SEPARATOR
                    + "s"
                    + j,
                currentTime);
            count++;
          }
        }
        tsFilesReader.close();
        if (i < 2) {
          if (j == 0 || j == 2) {
            if (i == 0) {
              assertEquals(600, count);
            } else {
              assertEquals(800, count);
            }
          } else if (j == 1 || j == 4) {
            assertEquals(500, count);
          } else {
            assertEquals(300, count);
          }
        } else {
          if (j == 0 || j == 2 || j == 4) {
            assertEquals(300, count);
          } else {
            assertEquals(0, count);
          }
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
  public void testAlignedCrossSpaceCompactionWithFileTimeIndexResource() throws Exception {
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
    deleteTimeseriesInMManager(seriesPaths);

    for (TsFileResource resource : seqResources) {
      resource.degradeTimeIndex();
    }
    for (TsFileResource resource : unseqResources) {
      resource.degradeTimeIndex();
    }

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(resource -> resource == null);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (i == TsFileGeneratorUtils.getAlignDeviceOffset()
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(
                  iterator.currentTime() + 20000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if ((i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(
                  iterator.currentTime() + 10000,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
   * Total 4 seq files and 5 unseq files, each file has different nonAligned timeseries.
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
  public void testCrossSpaceCompactionWithFileTimeIndex() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    registerTimeseriesInMManger(4, 5, false);
    createFiles(2, 2, 3, 300, 0, 0, 50, 50, false, true);
    createFiles(2, 4, 5, 300, 700, 700, 50, 50, false, true);
    createFiles(3, 3, 4, 200, 20, 10020, 30, 30, false, false);
    createFiles(2, 1, 5, 100, 450, 20450, 0, 0, false, false);

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                seqResources,
                unseqResources,
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    // degrade time index
    for (TsFileResource resource : seqResources) {
      resource.degradeTimeIndex();
    }
    for (TsFileResource resource : unseqResources) {
      resource.degradeTimeIndex();
    }

    targetResources.addAll(
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ICompactionPerformer performer =
        new FastCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new FastCompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    tsFileManager.addAll(targetResources, true);
    targetResources.get(3).degradeTimeIndex();
    targetResources.get(2).degradeTimeIndex();
    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            tsFileManager.getOrCreateSequenceListByTimePartition(0).getArrayList()));

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertFalse(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    for (int i = 2; i < 4; i++) {
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
      Assert.assertTrue(
          targetResources
              .get(i)
              .isDeviceIdExist(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 5; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                targetResources,
                new ArrayList<>(),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (measurementMaxTime.get(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            if (i == 0
                && ((450 <= iterator.currentTime() && iterator.currentTime() < 550)
                    || (550 <= iterator.currentTime() && iterator.currentTime() < 650))) {
              assertEquals(iterator.currentTime() + 20000, iterator.currentValue());
            } else if ((i < 3 && j < 4)
                && ((20 <= iterator.currentTime() && iterator.currentTime() < 220)
                    || (250 <= iterator.currentTime() && iterator.currentTime() < 450)
                    || (480 <= iterator.currentTime() && iterator.currentTime() < 680))) {
              assertEquals(iterator.currentTime() + 10000, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

  @Test
  public void testReleaseFileNumAndMemoryAfterCrossTask()
      throws IOException, MetadataException, WriteProcessException, InterruptedException {
    int oldMaxCrossCompactionCandidateFileNum =
        SystemInfo.getInstance().getTotalFileLimitForCompaction();
    SystemInfo.getInstance().setTotalFileLimitForCompactionTask(15);
    SystemInfo.getInstance().getCompactionFileNumCost().set(0);
    SystemInfo.getInstance().getCompactionMemoryCost().set(0);
    try {
      createFiles(6, 2, 3, 300, 0, 0, 50, 50, false, true);
      createFiles(6, 2, 3, 300, 0, 0, 50, 50, false, false);
      tsFileManager.addAll(seqResources, true);
      tsFileManager.addAll(unseqResources, false);
      CrossSpaceCompactionTask task =
          new CrossSpaceCompactionTask(
              0L,
              tsFileManager,
              seqResources,
              unseqResources,
              new FastCompactionPerformer(true),
              1000,
              0);
      Assert.assertTrue(task.setSourceFilesToCompactionCandidate());

      FixedPriorityBlockingQueue<AbstractCompactionTask> queue =
          new CompactionTaskQueue(50, new DefaultCompactionTaskComparatorImpl());
      queue.put(task);
      CompactionWorker worker = new CompactionWorker(0, queue);
      AbstractCompactionTask takeTask = queue.take();
      Assert.assertNotNull(takeTask);
      worker.processOneCompactionTask(takeTask);
      Assert.assertEquals(0, SystemInfo.getInstance().getCompactionFileNumCost().get());
      Assert.assertEquals(0, SystemInfo.getInstance().getCompactionMemoryCost().get());
    } finally {
      SystemInfo.getInstance()
          .setTotalFileLimitForCompactionTask(oldMaxCrossCompactionCandidateFileNum);
    }
  }

  private void validateSeqFiles() {
    TsFileValidationTool.clearMap(true);
    List<File> files = new ArrayList<>();
    for (TsFileResource resource : targetResources) {
      files.add(resource.getTsFile());
    }
    TsFileValidationTool.findIncorrectFiles(files);
    Assert.assertEquals(0, TsFileValidationTool.getBadFileNum());
  }
}
