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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.IBatchDataIterator;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class ReadPointCompactionPerformerTest extends AbstractCompactionTest {
  private final String oldThreadName = Thread.currentThread().getName();

  private final ICompactionPerformer performer = new ReadPointCompactionPerformer();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(2);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
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
        assertEquals(iterator.currentTime(), iterator.currentValue());
        count++;
        iterator.next();
      }
    }

    tsBlockReader.close();
    assertEquals(500, count);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    assertEquals(
        0,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
    assertEquals(
        0,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
    assertEquals(
        250,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
    assertEquals(
        600,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
    assertEquals(
        600,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
    for (int i = 0; i < 5; i++) {
      assertEquals(
          749,
          targetResources
              .get(0)
              .getEndTime(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i)));
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
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

  /*
  Total 6 seq files, each file has different nonAligned timeseries.
  First and Second file: d0 ~ d1 and s0 ~ s2, time range is 0 ~ 99 and 150 ~ 249, value range is  0 ~ 99 and 150 ~ 249.
  Third and Forth file: d0 ~ d2 and s0 ~ s4, time range is 250 ~ 299 and 350 ~ 399, value range is 250 ~ 299 and 350 ~ 399.
  Fifth and Sixth file: d0 ~ d4 and s0 ~ s5, time range is 600 ~ 649 and 700 ~ 749, value range is 800 ~ 849 and 900 ~ 949.
  Timeseries d[0-4].s5 are deleted before compaction.
  */
  @Test
  public void testSeqInnerSpaceCompactionWithFileTimeIndex() throws Exception {
    registerTimeseriesInMManger(5, 5, false);
    createFiles(2, 2, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(2, 3, 5, 50, 250, 250, 50, 50, false, true);
    createFiles(2, 5, 6, 50, 600, 800, 50, 50, false, true);

    for (int i = 0; i < 5; i++) {
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

    // degrade time index
    for (TsFileResource resource : seqResources) {
      resource.degradeTimeIndex();
    }
    for (TsFileResource resource : unseqResources) {
      resource.degradeTimeIndex();
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    assertEquals(
        0,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d0")));
    assertEquals(
        0,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d1")));
    assertEquals(
        250,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d2")));
    assertEquals(
        600,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d3")));
    assertEquals(
        600,
        targetResources
            .get(0)
            .getStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d4")));
    for (int i = 0; i < 5; i++) {
      assertEquals(
          749,
          targetResources
              .get(0)
              .getEndTime(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i)));
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s" + j, TSDataType.INT64));
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
            assertEquals(iterator.currentTime(), iterator.currentValue());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(500, count);
      }
    }

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
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
                new ArrayList<>(),
                targetResources,
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
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
                new ArrayList<>(),
                targetResources,
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
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
                new ArrayList<>(),
                targetResources,
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
        IFullPath path =
            new NonAlignedFullPath(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
                new MeasurementSchema("s" + j, TSDataType.INT64));
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i),
            new MeasurementSchema("s" + j, TSDataType.INT64));
        IDataBlockReader tsBlockReader =
            new SeriesDataBlockReader(
                path,
                FragmentInstanceContext.createFragmentInstanceContextForCompaction(
                    EnvironmentUtils.TEST_QUERY_CONTEXT.getQueryId()),
                new ArrayList<>(),
                targetResources,
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
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        assertEquals(0, count);
      }
    }
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);
    targetResources.removeIf(resource -> resource == null);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {
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
                new ArrayList<>(),
                targetResources,
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 5;
        i++) {
      for (int j = 0; j < 7; j++) {
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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(unseqResources, false);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(
        targetResources, CompactionTaskType.INNER_SEQ, COMPACTION_TEST_SG);

    for (int i = TsFileGeneratorUtils.getAlignDeviceOffset();
        i < TsFileGeneratorUtils.getAlignDeviceOffset() + 2;
        i++) {
      for (int j = 0; j < 3; j++) {
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
                new ArrayList<>(),
                targetResources,
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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
    deleteTimeseriesInMManager(seriesPaths);

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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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
    deleteTimeseriesInMManager(seriesPaths);

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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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
    deleteTimeseriesInMManager(seriesPaths);

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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);

    Assert.assertEquals(4, targetResources.size());
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource resource = targetResources.get(i);
      if (i < 2) {
        Assert.assertEquals(TsFileResourceStatus.DELETED, resource.getStatus());
      } else {
        Assert.assertTrue(resource.getTsFile().exists());
      }
    }
    targetResources.removeIf(x -> x.isDeleted());
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
  public void testCrossSpaceCompactionWithAllDataDeletedInDeviceInSeqFiles() throws Exception {
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1350, 0, true, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(x -> x.isDeleted());
    Assert.assertEquals(2, targetResources.size());

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
    for (int i = 0; i < 2; i++) {
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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
  public void testAlignedCrossSpaceCompactionWithDifferentMeasurementsInDifferentSourceFiles2()
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
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
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 200, 400, 0, true, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    ICompactionPerformer performer =
        new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
    performer.setSummary(new CompactionTaskSummary());
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

    seqResources.forEach(TsFileResource::degradeTimeIndex);
    unseqResources.forEach(TsFileResource::degradeTimeIndex);

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
    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
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

  /** Different source files have different devices and measurements with different schemas. */
  @Test
  public void testCrossSpaceCompactionWithDifferentDevicesAndMeasurements() throws Exception {
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
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d3" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    deleteTimeseriesInMManager(seriesPaths);
    setDataType(TSDataType.TEXT);
    registerTimeseriesInMManger(2, 7, false);
    List<Integer> deviceIndex = new ArrayList<>();
    deviceIndex.add(1);
    deviceIndex.add(3);
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      measurementIndex.add(i);
    }

    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1450, 0, false, true);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1350, 0, false, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(x -> x.isDeleted());
    Assert.assertEquals(3, targetResources.size());

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
      } else {
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
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      TSDataType tsDataType = (i < 2 || i == 3) ? TSDataType.TEXT : TSDataType.INT64;
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
        if (i == 1 || i == 3) {
          assertEquals(400, count);
        } else if (i == 2) {
          if (j < 4) {
            assertEquals(1200, count);
          } else if (j < 5) {
            assertEquals(600, count);
          } else {
            assertEquals(0, count);
          }
        } else {
          assertEquals(0, count);
        }
      }
    }
  }

  /**
   * Different source files have different aligned devices and measurements with different schemas.
   */
  @Test
  public void testAlignedCrossSpaceCompactionWithDifferentDevicesAndMeasurements()
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
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10001" + PATH_SEPARATOR + "s" + i);
      seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003" + PATH_SEPARATOR + "s" + i);
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    setDataType(TSDataType.TEXT);
    List<Integer> deviceIndex = new ArrayList<>();
    deviceIndex.add(1);
    deviceIndex.add(3);
    List<Integer> measurementIndex = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      measurementIndex.add(i);
    }

    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1450, 0, true, true);
    createFilesWithTextValue(1, deviceIndex, measurementIndex, 300, 1350, 0, true, false);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    performer.setTargetFiles(targetResources);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertEquals(0, FileReaderManager.getInstance().getClosedFileReaderMap().size());
    Assert.assertEquals(0, FileReaderManager.getInstance().getUnclosedFileReaderMap().size());
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    targetResources.removeIf(x -> x.isDeleted());
    Assert.assertEquals(3, targetResources.size());

    List<IDeviceID> deviceIdList = new ArrayList<>();
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10001"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10002"));
    deviceIdList.add(
        IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003"));
    for (int i = 0; i < 3; i++) {
      if (i < 2) {
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d10000")));
        Assert.assertFalse(
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
        Assert.assertFalse(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003")));
      } else {
        Assert.assertFalse(
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
        Assert.assertTrue(
            targetResources
                .get(i)
                .isDeviceIdExist(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(
                        COMPACTION_TEST_SG + PATH_SEPARATOR + "d10003")));
      }
      check(targetResources.get(i), deviceIdList);
    }

    Map<String, Long> measurementMaxTime = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      TSDataType tsDataType = i < 2 ? TSDataType.TEXT : TSDataType.INT64;
      for (int j = 0; j < 7; j++) {
        measurementMaxTime.putIfAbsent(
            COMPACTION_TEST_SG + PATH_SEPARATOR + "d1000" + i + PATH_SEPARATOR + "s" + j,
            Long.MIN_VALUE);
        List<IMeasurementSchema> schemas = new ArrayList<>();
        TSDataType dataType = i == 1 || i == 3 ? TSDataType.TEXT : TSDataType.INT64;
        schemas.add(new MeasurementSchema("s" + j, dataType));
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
                    COMPACTION_TEST_SG + PATH_SEPARATOR + "d1000" + i + PATH_SEPARATOR + "s" + j)
                >= iterator.currentTime()) {
              Assert.fail();
            }
            measurementMaxTime.put(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d1000" + i + PATH_SEPARATOR + "s" + j,
                iterator.currentTime());
            count++;
            iterator.next();
          }
        }
        tsBlockReader.close();
        if (i == 1 || i == 3) {
          assertEquals(400, count);
        } else if (i == 2) {
          if (j < 4) {
            assertEquals(1200, count);
          } else if (j < 5) {
            assertEquals(600, count);
          } else {
            assertEquals(0, count);
          }
        } else {
          assertEquals(0, count);
        }
      }
    }
  }

  @Test
  public void testCrossSpaceCompactionWithNewDeviceInUnseqFile() throws ExecutionException {
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
          new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
      performer.setSummary(new CompactionTaskSummary());
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
  public void testCrossSpaceCompactionWithDeviceMaxTimeLaterInUnseqFile()
      throws ExecutionException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    try {
      registerTimeseriesInMManger(6, 6, false);
      createFiles(2, 2, 3, 200, 0, 0, 0, 0, false, true);
      createFiles(3, 4, 4, 300, 20, 10020, 0, 0, false, false);

      List<TsFileResource> targetResources =
          CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
      ICompactionPerformer performer =
          new ReadPointCompactionPerformer(seqResources, unseqResources, targetResources);
      performer.setSummary(new CompactionTaskSummary());
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
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
