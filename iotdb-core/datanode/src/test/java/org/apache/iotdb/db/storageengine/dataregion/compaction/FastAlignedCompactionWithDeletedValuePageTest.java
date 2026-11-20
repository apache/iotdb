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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Verify that fast compaction does not copy the value pages that are deleted completely. */
public class FastAlignedCompactionWithDeletedValuePageTest extends AbstractCompactionTest {

  private static final String DEVICE = "d0";
  private static final List<String> MEASUREMENTS = Arrays.asList("s0", "s1", "s2");
  // one chunk with two pages, both of them are large enough to be flushed directly
  private static final long FIRST_PAGE_START_TIME = 1;
  private static final long FIRST_PAGE_END_TIME = 1000;
  private static final long SECOND_PAGE_START_TIME = 2001;
  private static final long SECOND_PAGE_END_TIME = 3000;

  private FastCompactionTaskSummary summary;

  @Before
  public void setUp()
      throws IOException, MetadataException, InterruptedException, WriteProcessException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testFlushPageWithFullyDeletedValuePage() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(100);
    unseqResources.add(createAlignedResource(false, Collections.singletonList("s1")));

    TsFileResource targetResource = performInnerCompaction();

    Assert.assertEquals(0, summary.pageNoneOverlapButDeserialize);
    validate(targetResource, readRows(Collections.emptyList(), unseqResources));
  }

  @Test
  public void testFlushPageWithFullyDeletedValuePageInBatch() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(2);
    unseqResources.add(createAlignedResource(false, Arrays.asList("s1", "s2")));

    TsFileResource targetResource = performInnerCompaction();

    validate(targetResource, readRows(Collections.emptyList(), unseqResources));
  }

  @Test
  public void testFlushPageWithFullyDeletedValuePageInCrossSpaceCompaction() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setCompactionMaxAlignedSeriesNumInOneBatch(100);
    seqResources.add(createAlignedResource(true, Collections.singletonList("s1")));
    unseqResources.add(
        generateSingleAlignedSeriesFile(
            DEVICE,
            MEASUREMENTS,
            new TimeRange[][] {new TimeRange[] {new TimeRange(5001, 6000)}},
            TSEncoding.RLE,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            false));
    List<TimeValuePair> expected = readRows(seqResources, unseqResources);

    Assert.assertTrue(
        new CrossSpaceCompactionTask(
                0,
                tsFileManager,
                seqResources,
                unseqResources,
                new FastCompactionPerformer(true),
                0,
                0)
            .start());

    List<TsFileResource> targetResources = tsFileManager.getTsFileList(true);
    Assert.assertEquals(1, targetResources.size());
    validate(targetResources.get(0), expected);
  }

  private TsFileResource createAlignedResource(boolean isSeq, List<String> deletedMeasurements)
      throws IOException, IllegalPathException {
    TsFileResource resource =
        generateSingleAlignedSeriesFile(
            DEVICE,
            MEASUREMENTS,
            new TimeRange[][] {
              new TimeRange[] {
                new TimeRange(FIRST_PAGE_START_TIME, FIRST_PAGE_END_TIME),
                new TimeRange(SECOND_PAGE_START_TIME, SECOND_PAGE_END_TIME)
              }
            },
            TSEncoding.RLE,
            CompressionType.LZ4,
            Arrays.asList(false, false, false),
            isSeq);
    for (String measurement : deletedMeasurements) {
      resource
          .getModFileForWrite()
          .write(
              new TreeDeletionEntry(
                  new MeasurementPath(COMPACTION_TEST_SG + "." + DEVICE, measurement),
                  FIRST_PAGE_END_TIME));
    }
    resource.getModFileForWrite().close();
    return resource;
  }

  private TsFileResource performInnerCompaction() throws Exception {
    tsFileManager.addAll(unseqResources, false);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(unseqResources, false);
    FastCompactionPerformer performer = new FastCompactionPerformer(false);
    summary = new FastCompactionTaskSummary();
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

  private void validate(TsFileResource targetResource, List<TimeValuePair> expected)
      throws Exception {
    TsFileResourceUtils.validateTsFileDataCorrectness(targetResource);
    List<TimeValuePair> actual =
        readRows(Collections.singletonList(targetResource), Collections.emptyList());
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i).getTimestamp(), actual.get(i).getTimestamp());
      Assert.assertArrayEquals(expected.get(i).getValues(), actual.get(i).getValues());
    }
  }

  private List<TimeValuePair> readRows(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws Exception {
    IFullPath path = getPaths(seqResources.isEmpty() ? unseqResources : seqResources).get(0);
    return CompactionCheckerUtils.getDataByQuery(
            Collections.singletonList(path), seqResources, unseqResources)
        .get(path);
  }
}
