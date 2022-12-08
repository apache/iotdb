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
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class FastInnerCompactionPerformerTest extends AbstractCompactionTest {

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
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  /* Total 5 seq files, each file has the same 6 nonAligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testSeqInnerSpaceCompactionWithSameTimeseries() throws Exception {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, false, true);

    PartialPath path =
        new MeasurementPath(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1",
            "s1",
            new MeasurementSchema("s1", TSDataType.INT64));
    IDataBlockReader tsBlockReader =
        new SeriesDataBlockReader(
            path,
            TSDataType.INT64,
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
        assertEquals(iterator.currentTime(), iterator.currentValue());
        count++;
        iterator.next();
      }
    }

    tsBlockReader.close();
    assertEquals(500, count);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            true,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();

    validateSeqFiles(true);

    tsBlockReader =
        new SeriesDataBlockReader(
            path,
            TSDataType.INT64,
            FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
            tsFileManager.getTsFileList(true),
            tsFileManager.getTsFileList(false),
            true);

    count = 0;
    while (tsBlockReader.hasNextBatch()) {
      TsBlock block = tsBlockReader.nextBatch();
      IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
      while (iterator.hasNext()) {
        assertEquals(iterator.currentTime(), iterator.currentValue());
        count++;
        iterator.next();
      }
    }

    tsBlockReader.close();
    assertEquals(500, count);
  }

  /*
  Total 6 seq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s5, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  Timeseries d[0-4].s5 are deleted before compaction.
  */
  @Test
  public void testSeqInnerSpaceCompactionWithDifferentTimeseries() throws Exception {
    registerTimeseriesInMManger(5, 5, false);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    createFiles(2, 5, 6, 50, 600, 800, 50, 50, false, true);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
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
            if (iterator.currentTime() >= 600) {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }

        tsBlockReader.close();
        if (i < 2 && j < 3) {
          assertEquals(400, count);
        } else if (i < 3) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            true,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    List<TsFileResource> targetResources = tsFileManager.getTsFileList(true);
    validateSeqFiles(true);

    assertEquals(
        0, targetResources.get(0).getStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d0"));
    assertEquals(
        0, targetResources.get(0).getStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d1"));
    assertEquals(
        250, targetResources.get(0).getStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d2"));
    assertEquals(
        600, targetResources.get(0).getStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3"));
    assertEquals(
        600, targetResources.get(0).getStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d4"));
    for (int i = 0; i < 5; i++) {
      assertEquals(
          749, targetResources.get(0).getEndTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i));
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();

          while (iterator.hasNext()) {
            if (iterator.currentTime() >= 600) {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
  public void testUnSeqInnerSpaceCompactionWithSameTimeseries() throws Exception {
    registerTimeseriesInMManger(2, 3, false);
    createFiles(5, 2, 3, 100, 0, 0, 50, 50, false, false);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s1",
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
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
            assertEquals(iterator.currentTime(), iterator.currentValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(500, count);
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    List<TsFileResource> targetResources = tsFileManager.getTsFileList(false);
    validateSeqFiles(false);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s1",
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            assertEquals(iterator.currentTime(), iterator.currentValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
  public void testUnSeqInnerSpaceCompactionWithDifferentTimeseries() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
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
            if ((100 <= iterator.currentTime() && iterator.currentTime() < 170)
                || (270 <= iterator.currentTime() && iterator.currentTime() < 340)) {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            } else if ((200 <= iterator.currentTime() && iterator.currentTime() < 270)
                || (370 <= iterator.currentTime() && iterator.currentTime() < 440)) {
              assertEquals(iterator.currentTime() + 100, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(true);

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));

        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if ((100 <= iterator.currentTime() && iterator.currentTime() < 170)
                || (270 <= iterator.currentTime() && iterator.currentTime() < 340)) {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            } else if ((200 <= iterator.currentTime() && iterator.currentTime() < 270)
                || (370 <= iterator.currentTime() && iterator.currentTime() < 440)) {
              assertEquals(iterator.currentTime() + 100, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
  public void testUnSeqInnerSpaceCompactionWithAllDataDeletedInTimeseries() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
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
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(iterator.currentTime() + 100, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(false);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(iterator.currentTime() + 100, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
  public void testUnSeqInnerSpaceCompactionWithAllDataDeletedInDevice() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
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
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(iterator.currentTime() + 100, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(false);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(iterator.currentTime(), iterator.currentValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(iterator.currentTime() + 100, iterator.currentValue());
            } else {
              assertEquals(iterator.currentTime() + 200, iterator.currentValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
  public void testUnSeqInnerSpaceCompactionWithAllDataDeletedInTargetFile() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
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
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(0, count);
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(true);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        PartialPath path =
            new MeasurementPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i,
                "s" + j,
                new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.INT64,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockSingleColumnIterator();
          while (iterator.hasNext()) {
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(0, count);
      }
    }
  }

  /* Total 5 seq files, each file has the same 6 aligned timeseries, each timeseries has the same 100 data point.*/
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithSameTimeseries() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            assertEquals(
                iterator.currentTime(),
                ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(500, count);
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            true,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    List<TsFileResource> targetResources = tsFileManager.getTsFileList(true);
    validateSeqFiles(true);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            assertEquals(
                iterator.currentTime(),
                ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(500, count);
      }
    }
  }

  /*
  Total 6 seq files, each file has different aligned timeseries, which cause empty page.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s7, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  Timeseries d[0-4].s7 are deleted before compaction.
  */
  @Test
  public void testAlignedSeqInnerSpaceCompactionWithDifferentTimeseriesAndEmptyPage()
      throws Exception {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(50);
    registerTimeseriesInMManger(5, 7, true);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, true, true);
    createFiles(2, 5, 8, 50, 600, 800, 50, 50, true, true);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            if (iterator.currentTime() >= 600) {
              assertEquals(
                  iterator.currentTime() + 200,
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
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            true,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(true);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() >= 600) {
              assertEquals(
                  iterator.currentTime() + 200,
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
      throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            if (iterator.currentTime() >= 600) {
              assertEquals(
                  iterator.currentTime() + 200,
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
          assertEquals(400, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(200, count);
        } else {
          assertEquals(100, count);
        }
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            true,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(true);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                false);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() >= 600) {
              assertEquals(
                  iterator.currentTime() + 200,
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
  public void testAlignedUnSeqInnerSpaceCompactionWithEmptyChunkAndEmptyPage() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(
                  iterator.currentTime() + 100,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime() + 200,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2 && j < 3) {
          assertEquals(1450, count);
        } else if (i < TsFileGeneratorUtils.getAlignDeviceOffset() + 3 && j < 5) {
          assertEquals(1200, count);
        } else {
          assertEquals(600, count);
        }
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(false);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(
                  iterator.currentTime() + 100,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime() + 200,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
      throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(
                  iterator.currentTime() + 100,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime() + 200,
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

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(false);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(
                  iterator.currentTime() + 100,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime() + 200,
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
  public void testAlignedUnSeqInnerSpaceCompactionWithAllDataDeletedInDevice() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(
                  iterator.currentTime() + 100,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime() + 200,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(false);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            if (iterator.currentTime() < 200
                || (iterator.currentTime() < 550 && iterator.currentTime() >= 500)) {
              assertEquals(
                  iterator.currentTime(),
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else if (iterator.currentTime() < 850) {
              assertEquals(
                  iterator.currentTime() + 100,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            } else {
              assertEquals(
                  iterator.currentTime() + 200,
                  ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            }
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
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
  public void testAlignedUnSeqInnerSpaceCompactionWithSameTimeseries() throws Exception {
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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
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
            assertEquals(
                iterator.currentTime(),
                ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(500, count);
      }
    }

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            unseqResources,
            false,
            new FastCompactionPerformer(false),
            new AtomicInteger(0),
            0);
    task.start();
    validateSeqFiles(false);

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
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                TSDataType.VECTOR,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                tsFileManager.getTsFileList(true),
                tsFileManager.getTsFileList(false),
                true);
        int count = 0;
        while (tsBlockReader.hasNextBatch()) {
          TsBlock block = tsBlockReader.nextBatch();
          IBatchDataIterator iterator = block.getTsBlockAlignedRowIterator();
          while (iterator.hasNext()) {
            assertEquals(
                iterator.currentTime(),
                ((TsPrimitiveType[]) (iterator.currentValue()))[0].getValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(500, count);
      }
    }
  }
}
