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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BatchedAlignedSeriesCrossSpaceCompactionTest extends AbstractCompactionTest {

  long originTargetChunkSize;
  long originTargetChunkPointNum;
  int originTargetPageSize;
  int originTargetPagePointNum;
  int originMaxConcurrentAlignedSeriesInCompaction;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
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
  public void testFlushChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(700000, 800000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(500000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testFlushChunkWithEmptyChunk() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(700000, 800000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(500000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testFlushChunkWithDeletion() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(100000, 200000), new TimeRange(300000, 400000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);

    seqResource1
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.d0", "*"), Long.MAX_VALUE, 200000));
    seqResource1.getModFile().close();
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(700000, 800000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[] {new TimeRange(500000, 600000)},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testFlushPage() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(1000, 2000), new TimeRange(5000, 6000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(9000, 10000)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(3000, 4000), new TimeRange(7000, 8000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testFlushEmptyPage() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(1000, 2000), new TimeRange(5000, 6000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(9000, 10000)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {new TimeRange(3000, 4000), new TimeRange(7000, 8000)}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testCompactByDeserializePage() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(50, 60)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(90, 100)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(30, 40), new TimeRange(70, 80)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testCompactByDeserializePageWithPartialDeletion() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(50, 60)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResource1
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.d0", "s0"), Long.MAX_VALUE, 15));
    seqResource1
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.d0", "s2"), Long.MAX_VALUE, 20));
    seqResource1.getModFile().close();
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(90, 100)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(30, 40), new TimeRange(70, 80)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testCompactByDeserializePageWithEmpty() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(50, 60)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(90, 100)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, true),
            true);
    seqResources.add(seqResource2);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(30, 40), new TimeRange(70, 80)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testCompactByDeserialize() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {
                new TimeRange(1000, 2000), new TimeRange(50000, 60000), new TimeRange(60001, 60002)
              }
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 20), new TimeRange(50, 60)}},
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  @Test
  public void testCompactByFlushChunkAndDeserialize() throws Exception {
    TsFileResource seqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {
              new TimeRange[] {
                new TimeRange(1000, 2000), new TimeRange(50000, 60000), new TimeRange(60001, 60002)
              }
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            true);
    seqResources.add(seqResource1);

    TsFileResource unseqResource1 =
        generateSingleAlignedSeriesFile(
            "d0",
            Arrays.asList("s0", "s1", "s2"),
            new TimeRange[][] {new TimeRange[] {new TimeRange(10000, 20000)}},
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            Arrays.asList(false, false, false),
            false);
    unseqResources.add(unseqResource1);
    List<TsFileResource> targetResources = performCompaction();
    validate(targetResources);
  }

  private List<TsFileResource> performCompaction() throws Exception {
    tsFileManager.addAll(unseqResources, false);
    List<TsFileResource> targetResources =
        TsFileNameGenerator.getCrossCompactionTargetFileResources(seqResources);

    FastCompactionPerformer performer = new FastCompactionPerformer(true);
    FastCompactionTaskSummary summary = new FastCompactionTaskSummary();
    performer.setSummary(summary);
    performer.setSourceFiles(seqResources, unseqResources);
    performer.setTargetFiles(targetResources);
    performer.perform();
    CompactionUtils.moveTargetFile(targetResources, CompactionTaskType.CROSS, COMPACTION_TEST_SG);
    return targetResources;
  }

  private void validate(List<TsFileResource> targetResources)
      throws IllegalPathException, IOException {
    for (TsFileResource targetResource : targetResources) {
      TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    }
    List<TsFileResource> sourceTsFileResources =
        new ArrayList<>(seqResources.size() + unseqResources.size());
    sourceTsFileResources.addAll(seqResources);
    sourceTsFileResources.addAll(unseqResources);
    Assert.assertTrue(
        CompactionCheckerUtils.compareSourceDataAndTargetData(
            CompactionCheckerUtils.getDataByQuery(
                getPaths(sourceTsFileResources), seqResources, unseqResources),
            CompactionCheckerUtils.getDataByQuery(
                getPaths(targetResources), targetResources, Collections.emptyList())));
  }
}
