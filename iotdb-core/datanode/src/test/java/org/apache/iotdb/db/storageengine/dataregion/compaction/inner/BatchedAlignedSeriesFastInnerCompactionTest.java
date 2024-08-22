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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class BatchedAlignedSeriesFastInnerCompactionTest extends AbstractCompactionTest {

  long originTargetChunkSize;
  long originTargetChunkPointNum;
  int originTargetPageSize;
  int originTargetPagePointNum;
  int originMaxConcurrentAlignedSeriesInCompaction;

  @Before
  public void setUp()
      throws IOException, MetadataException, InterruptedException, WriteProcessException {
    super.setUp();
    originTargetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    originTargetChunkPointNum = IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    originTargetPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    originTargetPagePointNum =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    originMaxConcurrentAlignedSeriesInCompaction =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();

    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1048576);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100000);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(64 * 1024);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10000);
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(2);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(originTargetChunkSize);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(originTargetChunkPointNum);
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(originTargetPageSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originTargetPagePointNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionMaxAlignedSeriesNumInOneBatch(originMaxConcurrentAlignedSeriesInCompaction);
  }

  @Test
  public void testCompactionByFlushChunk() throws Exception {

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(500000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByFlushChunkWithDifferentCompressionType() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(500000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByFlushChunkWithEmptyChunk() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(500000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            Arrays.asList(false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByFlushPage() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300, 400)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByFlushPageWithDifferentCompressionType() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300, 400)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByFlushPageWithDeletion() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300, 450)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResource2
        .getModFile()
        .write(
            new Deletion(
                new MeasurementPath("root.testsg.d0", "s2"), Long.MAX_VALUE, Long.MAX_VALUE));
    unseqResource2.getModFile().close();
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByFlushPageWithEmptyPage() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1"),
            new TimeRange[] {new TimeRange(300, 400)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByDeserialize() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(150, 550)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByDeserializeWithEmptyColumn() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1"),
            new TimeRange[] {new TimeRange(150, 550)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false),
            false);
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  @Test
  public void testCompactionByDeserializePageWithDeletion() throws Exception {
    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(100, 200), new TimeRange(500, 600)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResource1
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.d0", "s2"), Long.MAX_VALUE, 150));
    unseqResource1.getModFile().close();
    unseqResources.add(unseqResource1);

    TsFileResource unseqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(300, 450)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResource2
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.d0", "s2"), Long.MAX_VALUE, 400));
    unseqResource2.getModFile().close();
    unseqResources.add(unseqResource2);

    TsFileResource targetResource = performCompaction();
    validate(targetResource);
  }

  private TsFileResource performCompaction() throws Exception {
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(unseqResources, false);

    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    FastCompactionTaskSummary summary = new FastCompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(unseqResources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource),
        CompactionTaskType.INNER_SEQ,
        COMPACTION_TEST_SG);
    return targetResource;
  }

  private void validate(TsFileResource targetResource) throws IllegalPathException, IOException {
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(unseqResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(Collections.singletonList(targetResource)),
                Collections.singletonList(targetResource),
                Collections.emptyList())));
  }
}
