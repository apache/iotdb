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

package org.apache.iotdb.db.pipe.source.dataregion.historical;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimePartitionProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.resource.PersistentResource;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeHistoricalDataRegionTsFileAndDeletionSourceTest {

  @Test
  public void testDeletionOnlyCustomizeInitializesSourceContext() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.EXTRACTOR_INCLUSION_KEY, "data.delete");
              }
            });

    source.validate(new PipeParameterValidator(parameters));
    source.customize(
        parameters,
        new PipeTaskRuntimeConfiguration(
            new PipeTaskSourceRuntimeEnvironment(
                "pipe", 1, 123, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1))));

    Assert.assertEquals("pipe", getPrivateField(source, "pipeName"));
    Assert.assertEquals(123, getPrivateField(source, "dataRegionId"));
    Assert.assertEquals(false, getPrivateField(source, "shouldExtractInsertion"));
    Assert.assertEquals(true, getPrivateField(source, "shouldExtractDeletion"));
    Assert.assertNotNull(getPrivateField(source, "treePattern"));
    Assert.assertNotNull(getPrivateField(source, "tablePattern"));
  }

  @Test
  public void testSupplyReturnsProgressReportEventAfterSkippingDuplicateHistoricalTsFile()
      throws Exception {
    final TestablePipeHistoricalDataRegionTsFileAndDeletionSource source =
        new TestablePipeHistoricalDataRegionTsFileAndDeletionSource();
    final Event expectedEvent = new Event() {};
    final File tempDir = Files.createTempDirectory("pipeHistoricalSkipDuplicate").toFile();

    try {
      final TsFileResource skippedResource = createTsFileResource(tempDir, "skip.tsfile");
      final TsFileResource nextResource = createTsFileResource(tempDir, "next.tsfile");

      source.setSkippedTsFilePaths(skippedResource.getTsFilePath());
      source.setSuppliedEvent(expectedEvent);
      setPrivateField(source, "hasBeenStarted", true);
      setPrivateField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(Arrays.asList(skippedResource, nextResource)));

      Assert.assertTrue(source.supply() instanceof ProgressReportEvent);
      Assert.assertEquals(
          Arrays.asList(skippedResource.getTsFilePath()), source.getConsumedSkippedTsFilePaths());
      Assert.assertTrue(source.getSuppliedTsFiles().isEmpty());
      Assert.assertEquals(1, source.getPendingQueueSize());

      Assert.assertSame(expectedEvent, source.supply());
      Assert.assertEquals(Arrays.asList(nextResource.getTsFilePath()), source.getSuppliedTsFiles());
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testSupplyDoesNotSwallowNonSkippedNullTsFileEvent() throws Exception {
    final TestablePipeHistoricalDataRegionTsFileAndDeletionSource source =
        new TestablePipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalNullSemantics").toFile();

    try {
      final TsFileResource firstResource = createTsFileResource(tempDir, "first.tsfile");
      final TsFileResource secondResource = createTsFileResource(tempDir, "second.tsfile");

      source.setSuppliedEvent(null);
      setPrivateField(source, "hasBeenStarted", true);
      setPrivateField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(Arrays.asList(firstResource, secondResource)));

      Assert.assertNull(source.supply());
      Assert.assertEquals(
          Arrays.asList(firstResource.getTsFilePath()), source.getSuppliedTsFiles());
      Assert.assertEquals(1, source.getPendingQueueSize());
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testSupplyRetriesSameTsFileAfterEventCreationFailure() throws Exception {
    final TestablePipeHistoricalDataRegionTsFileAndDeletionSource source =
        new TestablePipeHistoricalDataRegionTsFileAndDeletionSource();
    final Event expectedEvent = new Event() {};
    final RuntimeException expectedException = new RuntimeException("mock supply failure");
    final File tempDir = Files.createTempDirectory("pipeHistoricalRetry").toFile();

    try {
      final TsFileResource firstResource = createTsFileResource(tempDir, "first.tsfile");
      final TsFileResource secondResource = createTsFileResource(tempDir, "second.tsfile");

      source.setSuppliedEvent(expectedEvent);
      source.setFailureBeforeSuccess(expectedException, 1);
      setPrivateField(source, "hasBeenStarted", true);
      setPrivateField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(Arrays.asList(firstResource, secondResource)));

      final RuntimeException actualException =
          Assert.assertThrows(RuntimeException.class, source::supply);
      Assert.assertSame(expectedException, actualException);
      Assert.assertEquals(
          Arrays.asList(firstResource.getTsFilePath()), source.getSuppliedTsFiles());
      Assert.assertEquals(2, source.getPendingQueueSize());

      Assert.assertSame(expectedEvent, source.supply());
      Assert.assertEquals(
          Arrays.asList(firstResource.getTsFilePath(), firstResource.getTsFilePath()),
          source.getSuppliedTsFiles());
      Assert.assertEquals(1, source.getPendingQueueSize());
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderDefaultsToTrue() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();

    source.validate(new PipeParameterValidator(new PipeParameters(new HashMap<>())));

    Assert.assertTrue(
        (Boolean) getPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority"));
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderMatchesQueryCoverage() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileOrder").toFile();

    try {
      final TsFileResource seqLowerVersionNewerFileTimestamp =
          createTsFileResource(tempDir, "300-1-0-0.tsfile");
      seqLowerVersionNewerFileTimestamp.setSeq(true);
      final TsFileResource seqSameVersionOlderFileTimestamp =
          createTsFileResource(tempDir, "100-2-0-0.tsfile");
      seqSameVersionOlderFileTimestamp.setSeq(true);
      final TsFileResource seqSameVersionNewerFileTimestamp =
          createTsFileResource(tempDir, "200-2-0-0.tsfile");
      seqSameVersionNewerFileTimestamp.setSeq(true);
      final TsFileResource seqHigherVersionOlderFileTimestamp =
          createTsFileResource(tempDir, "50-3-0-0.tsfile");
      seqHigherVersionOlderFileTimestamp.setSeq(true);
      final TsFileResource unseqLowerVersionOldestFileTimestamp =
          createTsFileResource(tempDir, "1-1-0-0.tsfile");
      unseqLowerVersionOldestFileTimestamp.setSeq(false);

      setPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority", true);
      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "shouldExtractDeletion", false);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);

      final List<PersistentResource> resources =
          new ArrayList<>(
              Arrays.asList(
                  unseqLowerVersionOldestFileTimestamp,
                  seqHigherVersionOlderFileTimestamp,
                  seqSameVersionNewerFileTimestamp,
                  seqSameVersionOlderFileTimestamp,
                  seqLowerVersionNewerFileTimestamp));
      sortExtractedResources(source, resources);

      Assert.assertEquals(
          Arrays.asList(
              seqLowerVersionNewerFileTimestamp,
              seqSameVersionOlderFileTimestamp,
              seqSameVersionNewerFileTimestamp,
              seqHigherVersionOlderFileTimestamp,
              unseqLowerVersionOldestFileTimestamp),
          resources);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderCanBeDisabled() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(
                    PipeSourceConstant.SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                    Boolean.FALSE.toString());
              }
            });
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileProgressOrder").toFile();

    try {
      source.validate(new PipeParameterValidator(parameters));
      final TsFileResource earlierProgressIndex = createTsFileResource(tempDir, "300-1-0-0.tsfile");
      earlierProgressIndex.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource laterProgressIndex = createTsFileResource(tempDir, "100-1-0-0.tsfile");
      laterProgressIndex.updateProgressIndex(new SimpleProgressIndex(0, 2));

      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "shouldExtractDeletion", false);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);

      final List<PersistentResource> resources =
          new ArrayList<>(Arrays.asList(laterProgressIndex, earlierProgressIndex));
      sortExtractedResources(source, resources);

      Assert.assertFalse(
          (Boolean) getPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority"));
      Assert.assertEquals(Arrays.asList(earlierProgressIndex, laterProgressIndex), resources);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderCanBeDisabledByExtractorKey() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(
                    PipeSourceConstant.EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                    Boolean.FALSE.toString());
              }
            });

    source.validate(new PipeParameterValidator(parameters));

    Assert.assertFalse(
        (Boolean) getPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority"));
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderFallsBackWhenDeletionExtracted()
      throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir =
        Files.createTempDirectory("pipeHistoricalTsFileDeletionProgressOrder").toFile();

    try {
      final TsFileResource higherQueryPriorityEarlierProgress =
          createTsFileResource(tempDir, "300-3-0-0.tsfile");
      higherQueryPriorityEarlierProgress.setSeq(true);
      higherQueryPriorityEarlierProgress.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource lowerQueryPriorityLaterProgress =
          createTsFileResource(tempDir, "100-1-0-0.tsfile");
      lowerQueryPriorityLaterProgress.setSeq(true);
      lowerQueryPriorityLaterProgress.updateProgressIndex(new SimpleProgressIndex(0, 2));

      setPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority", true);
      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "shouldExtractDeletion", true);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);

      final List<PersistentResource> resources =
          new ArrayList<>(
              Arrays.asList(lowerQueryPriorityLaterProgress, higherQueryPriorityEarlierProgress));
      sortExtractedResources(source, resources);

      Assert.assertEquals(
          Arrays.asList(higherQueryPriorityEarlierProgress, lowerQueryPriorityLaterProgress),
          resources);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testQueryPriorityOrderReportsProgressAfterAllHistoricalResources() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final ProgressIndex expectedProgressIndex = new SimpleProgressIndex(0, 10);

    setPrivateField(source, "hasBeenStarted", true);
    setPrivateField(source, "pipeName", "pipe");
    setPrivateField(source, "creationTime", 1L);
    setPrivateField(source, "dataRegionId", 1);
    setPrivateField(source, "pipeTaskMeta", new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    setPrivateField(source, "pendingQueue", new ArrayDeque<PersistentResource>());
    setPrivateField(source, "maxHistoricalProgressIndex", expectedProgressIndex);
    setPrivateField(source, "shouldReportMaxHistoricalProgressIndex", true);

    final Event event = source.supply();

    Assert.assertTrue(event instanceof ProgressReportEvent);
    Assert.assertEquals(expectedProgressIndex, ((ProgressReportEvent) event).getProgressIndex());
    Assert.assertFalse((Boolean) getPrivateField(source, "shouldReportMaxHistoricalProgressIndex"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryPriorityOrderProgressOnlyCoversSelectedResources() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileSelectedProgress").toFile();

    try {
      final TsFileResource selectedResource = createTsFileResource(tempDir, "100-1-0-0.tsfile");
      selectedResource.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource filteredResource = createTsFileResource(tempDir, "200-1-0-0.tsfile");
      filteredResource.updateProgressIndex(new SimpleProgressIndex(0, 100));

      ((Map<TsFileResource, Set<String>>)
              getPrivateField(source, "filteredTsFileResources2TableNames"))
          .put(selectedResource, Set.of());

      final List<PersistentResource> resources =
          new ArrayList<>(Arrays.asList(filteredResource, selectedResource));
      prepareResourcesForHistoricalTsFileQueryPriorityOrder(source, resources);

      Assert.assertEquals(Arrays.asList(selectedResource), resources);
      Assert.assertEquals(
          new SimpleProgressIndex(0, 1), getPrivateField(source, "maxHistoricalProgressIndex"));
      Assert.assertTrue(
          (Boolean) getPrivateField(source, "shouldReportMaxHistoricalProgressIndex"));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryPriorityOrderPreparesIncrementalSafeProgressReports() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir =
        Files.createTempDirectory("pipeHistoricalTsFileIncrementalProgress").toFile();

    try {
      final TsFileResource progress1 = createTsFileResource(tempDir, "1.tsfile");
      progress1.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource progress2 = createTsFileResource(tempDir, "2.tsfile");
      progress2.updateProgressIndex(new SimpleProgressIndex(0, 2));
      final TsFileResource progress4 = createTsFileResource(tempDir, "4.tsfile");
      progress4.updateProgressIndex(new SimpleProgressIndex(0, 4));
      final TsFileResource progress3 = createTsFileResource(tempDir, "3.tsfile");
      progress3.updateProgressIndex(new SimpleProgressIndex(0, 3));
      final TsFileResource progress5 = createTsFileResource(tempDir, "5.tsfile");
      progress5.updateProgressIndex(new SimpleProgressIndex(0, 5));

      final List<PersistentResource> resources =
          new ArrayList<>(Arrays.asList(progress1, progress2, progress4, progress3, progress5));
      prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(source, resources);

      Assert.assertEquals(
          new HashSet<>(Arrays.asList(progress1, progress2, progress3, progress5)),
          getPrivateField(source, "historicalProgressReportResources"));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryPriorityOrderPreparesSafeProgressReportsByTimePartition() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir =
        Files.createTempDirectory("pipeHistoricalTsFilePartitionProgress").toFile();

    try {
      final TsFileResource partition0Progress100 =
          createTsFileResource(tempDir, 0L, "100-1-0-0.tsfile");
      partition0Progress100.updateProgressIndex(new SimpleProgressIndex(0, 100));
      final TsFileResource partition1Progress20 =
          createTsFileResource(tempDir, 1L, "20-1-0-0.tsfile");
      partition1Progress20.updateProgressIndex(new SimpleProgressIndex(0, 20));
      final List<PersistentResource> resources =
          new ArrayList<>(Arrays.asList(partition0Progress100, partition1Progress20));
      prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(source, resources);

      Assert.assertEquals(
          new HashSet<>(Arrays.asList(partition0Progress100, partition1Progress20)),
          getPrivateField(source, "historicalProgressReportResources"));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryPriorityOrderSuppliesPartitionProgressForOutOfGlobalOrderTsFiles()
      throws Exception {
    final TestablePipeHistoricalDataRegionTsFileAndDeletionSource source =
        new TestablePipeHistoricalDataRegionTsFileAndDeletionSource();
    final Event expectedEvent = new Event() {};
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileCrossPartition").toFile();

    try {
      final TsFileResource partition0Progress100 =
          createTsFileResource(tempDir, 0L, "100-1-0-0.tsfile");
      partition0Progress100.updateProgressIndex(new SimpleProgressIndex(0, 100));
      final TsFileResource partition1Progress20 =
          createTsFileResource(tempDir, 1L, "20-1-0-0.tsfile");
      partition1Progress20.updateProgressIndex(new SimpleProgressIndex(0, 20));

      source.setSuppliedEvent(expectedEvent);
      setPrivateField(source, "hasBeenStarted", true);
      setPrivateField(source, "pipeName", "pipe");
      setPrivateField(source, "creationTime", 1L);
      setPrivateField(source, "pipeTaskMeta", new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
      setPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority", true);
      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "shouldExtractDeletion", false);
      setPrivateField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(
              Arrays.asList(partition0Progress100, partition1Progress20)));
      ((Set<PersistentResource>) getPrivateField(source, "historicalProgressReportResources"))
          .add(partition0Progress100);
      ((Set<PersistentResource>) getPrivateField(source, "historicalProgressReportResources"))
          .add(partition1Progress20);

      Assert.assertSame(expectedEvent, source.supply());
      final Event partition0ProgressEvent = source.supply();
      Assert.assertTrue(partition0ProgressEvent instanceof ProgressReportEvent);
      final ProgressIndex partition0ProgressIndex =
          ((ProgressReportEvent) partition0ProgressEvent).getProgressIndex();
      Assert.assertEquals(
          new TimePartitionProgressIndex(
              partition0Progress100.getTimePartition(),
              partition0Progress100.getMaxProgressIndex()),
          partition0ProgressIndex);
      Assert.assertFalse(
          ((TimePartitionProgressIndex) partition0ProgressIndex)
              .isProgressIndexEqualOrAfter(
                  partition1Progress20.getTimePartition(),
                  partition1Progress20.getMaxProgressIndex()));

      Assert.assertSame(expectedEvent, source.supply());
      final Event partition1ProgressEvent = source.supply();
      Assert.assertTrue(partition1ProgressEvent instanceof ProgressReportEvent);
      Assert.assertEquals(
          new TimePartitionProgressIndex(
              partition1Progress20.getTimePartition(), partition1Progress20.getMaxProgressIndex()),
          ((ProgressReportEvent) partition1ProgressEvent).getProgressIndex());
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryPriorityOrderSuppliesProgressAfterSafeTsFileEvent() throws Exception {
    final TestablePipeHistoricalDataRegionTsFileAndDeletionSource source =
        new TestablePipeHistoricalDataRegionTsFileAndDeletionSource();
    final Event expectedEvent = new Event() {};
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileSafeProgress").toFile();

    try {
      final TsFileResource firstResource = createTsFileResource(tempDir, "first.tsfile");
      firstResource.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource secondResource = createTsFileResource(tempDir, "second.tsfile");
      secondResource.updateProgressIndex(new SimpleProgressIndex(0, 2));

      source.setSuppliedEvent(expectedEvent);
      setPrivateField(source, "hasBeenStarted", true);
      setPrivateField(source, "pipeName", "pipe");
      setPrivateField(source, "creationTime", 1L);
      setPrivateField(source, "pipeTaskMeta", new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
      setPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority", true);
      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "shouldExtractDeletion", false);
      setPrivateField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(Arrays.asList(firstResource, secondResource)));
      ((Set<PersistentResource>) getPrivateField(source, "historicalProgressReportResources"))
          .add(firstResource);

      Assert.assertSame(expectedEvent, source.supply());
      final Event progressEvent = source.supply();
      Assert.assertTrue(progressEvent instanceof ProgressReportEvent);
      Assert.assertEquals(
          new TimePartitionProgressIndex(
              firstResource.getTimePartition(), firstResource.getMaxProgressIndex()),
          ((ProgressReportEvent) progressEvent).getProgressIndex());
      Assert.assertSame(expectedEvent, source.supply());
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testReplicateIndexShouldBeStableBeforeResourceConsumed() throws Exception {
    final TestablePipeHistoricalDataRegionTsFileAndDeletionSource source =
        new TestablePipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalReplicateIndex").toFile();

    try {
      final TsFileResource resource = createTsFileResource(tempDir, "stable.tsfile");
      final String pipeName = "consensus_pipe_retry_test_" + System.nanoTime();
      setPrivateField(source, "pipeName", pipeName);
      ReplicateProgressDataNodeManager.resetReplicateIndexForIoTV2(pipeName);

      Assert.assertEquals(1L, source.assignReplicateIndexForResource(resource));
      Assert.assertEquals(1L, source.assignReplicateIndexForResource(resource));

      source.clearReplicateIndexForResource(resource);
      Assert.assertEquals(2L, source.assignReplicateIndexForResource(resource));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testMayTsFileContainUnprocessedDataUsesEqualOrAfterCoverage() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalProgressCoverage").toFile();

    try {
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "superset.tsfile",
          hybridProgressIndex(
              new IoTProgressIndex(Map.of(1, 100L, 2, 200L)),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10))),
          hybridProgressIndex(
              new IoTProgressIndex(Map.of(1, 100L)),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 9))),
          false);

      assertMayTsFileContainUnprocessedData(
          tempDir,
          "missing-dimension.tsfile",
          hybridProgressIndex(new IoTProgressIndex(Map.of(1, 100L))),
          hybridProgressIndex(
              new IoTProgressIndex(Map.of(1, 90L)),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10))),
          true);

      assertMayTsFileContainUnprocessedData(
          tempDir,
          "larger-iot.tsfile",
          hybridProgressIndex(
              new IoTProgressIndex(Map.of(1, 100L, 2, 200L)),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10))),
          hybridProgressIndex(
              new IoTProgressIndex(Map.of(1, 101L)),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10))),
          true);

      final ProgressIndex recoverProgressIndex =
          new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10));
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "old-sequence-recover.tsfile",
          hybridProgressIndex(recoverProgressIndex, new IoTProgressIndex(Map.of(1, 100L))),
          recoverProgressIndex,
          false);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testMayTsFileContainUnprocessedDataUsesTimePartitionProgressCoverage()
      throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalPartitionCoverage").toFile();

    try {
      final ProgressIndex startIndex =
          new TimePartitionProgressIndex(0L, new SimpleProgressIndex(0, 100));
      assertMayTsFileContainUnprocessedData(
          tempDir,
          0L,
          "partition-covered.tsfile",
          startIndex,
          new SimpleProgressIndex(0, 50),
          false);
      assertMayTsFileContainUnprocessedData(
          tempDir,
          1L,
          "partition-uncovered.tsfile",
          startIndex,
          new SimpleProgressIndex(0, 50),
          true);

      final ProgressIndex hybridStartIndex =
          hybridProgressIndex(
              startIndex, new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 1)));
      assertMayTsFileContainUnprocessedData(
          tempDir,
          0L,
          "hybrid-partition-covered.tsfile",
          hybridStartIndex,
          new SimpleProgressIndex(0, 80),
          false);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  private static TsFileResource createTsFileResource(final File tempDir, final String fileName)
      throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());
    return new TsFileResource(file);
  }

  private static TsFileResource createTsFileResource(
      final File tempDir, final long timePartitionId, final String fileName) throws IOException {
    final File regionDir = new File(tempDir, "1");
    final File partitionDir = new File(regionDir, String.valueOf(timePartitionId));
    Assert.assertTrue(partitionDir.exists() || partitionDir.mkdirs());
    return createTsFileResource(partitionDir, fileName);
  }

  private static TsFileResource createClosedTsFileResource(
      final File tempDir, final String fileName, final ProgressIndex progressIndex)
      throws IOException {
    final TsFileResource resource = createTsFileResource(tempDir, fileName);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.updateProgressIndex(progressIndex);
    return resource;
  }

  private static TsFileResource createClosedTsFileResource(
      final File tempDir,
      final long timePartitionId,
      final String fileName,
      final ProgressIndex progressIndex)
      throws IOException {
    final TsFileResource resource = createTsFileResource(tempDir, timePartitionId, fileName);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.updateProgressIndex(progressIndex);
    return resource;
  }

  private static void assertMayTsFileContainUnprocessedData(
      final File tempDir,
      final String fileName,
      final ProgressIndex startIndex,
      final ProgressIndex resourceProgressIndex,
      final boolean expected)
      throws Exception {
    assertMayTsFileContainUnprocessedData(
        startIndex, createClosedTsFileResource(tempDir, fileName, resourceProgressIndex), expected);
  }

  private static void assertMayTsFileContainUnprocessedData(
      final File tempDir,
      final long timePartitionId,
      final String fileName,
      final ProgressIndex startIndex,
      final ProgressIndex resourceProgressIndex,
      final boolean expected)
      throws Exception {
    assertMayTsFileContainUnprocessedData(
        startIndex,
        createClosedTsFileResource(tempDir, timePartitionId, fileName, resourceProgressIndex),
        expected);
  }

  private static void assertMayTsFileContainUnprocessedData(
      final ProgressIndex startIndex, final TsFileResource resource, final boolean expected)
      throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    setPrivateField(source, "pipeName", "pipe");
    setPrivateField(source, "dataRegionId", 1);
    setPrivateField(source, "startIndex", startIndex);

    final Method method =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
            "mayTsFileContainUnprocessedData", TsFileResource.class);
    method.setAccessible(true);
    Assert.assertEquals(expected, method.invoke(source, resource));
  }

  private static ProgressIndex hybridProgressIndex(
      final ProgressIndex firstProgressIndex, final ProgressIndex... progressIndexes) {
    ProgressIndex result = new HybridProgressIndex(firstProgressIndex);
    for (final ProgressIndex progressIndex : progressIndexes) {
      result = result.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
    }
    return result;
  }

  private static void sortExtractedResources(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source,
      final List<PersistentResource> resources)
      throws ReflectiveOperationException {
    final Method method =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
            "sortExtractedResources", List.class);
    method.setAccessible(true);
    method.invoke(source, resources);
  }

  private static void prepareResourcesForHistoricalTsFileQueryPriorityOrder(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source,
      final List<PersistentResource> resources)
      throws ReflectiveOperationException {
    final Method method =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
            "prepareResourcesForHistoricalTsFileQueryPriorityOrder", List.class);
    method.setAccessible(true);
    method.invoke(source, resources);
  }

  private static void prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source,
      final List<PersistentResource> resources)
      throws ReflectiveOperationException {
    final Method method =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
            "prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder", List.class);
    method.setAccessible(true);
    method.invoke(source, resources);
  }

  private static void setPrivateField(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source,
      final String fieldName,
      final Object value)
      throws ReflectiveOperationException {
    final Field field =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(source, value);
  }

  private static Object getPrivateField(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source, final String fieldName)
      throws ReflectiveOperationException {
    final Field field =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(source);
  }

  private static class TestablePipeHistoricalDataRegionTsFileAndDeletionSource
      extends PipeHistoricalDataRegionTsFileAndDeletionSource {

    private final Set<String> skippedTsFilePaths = new HashSet<>();
    private final List<String> consumedSkippedTsFilePaths = new ArrayList<>();
    private final List<String> suppliedTsFiles = new ArrayList<>();
    private Event suppliedEvent;
    private RuntimeException exceptionToThrow;
    private int remainingFailureCount;

    private void setSkippedTsFilePaths(final String... skippedTsFilePaths) {
      this.skippedTsFilePaths.clear();
      this.skippedTsFilePaths.addAll(Arrays.asList(skippedTsFilePaths));
    }

    private List<String> getConsumedSkippedTsFilePaths() {
      return consumedSkippedTsFilePaths;
    }

    private List<String> getSuppliedTsFiles() {
      return suppliedTsFiles;
    }

    @Override
    public int getPendingQueueSize() {
      try {
        final Field field =
            PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredField("pendingQueue");
        field.setAccessible(true);
        return ((ArrayDeque<?>) field.get(this)).size();
      } catch (final ReflectiveOperationException e) {
        throw new AssertionError(e);
      }
    }

    private void setSuppliedEvent(final Event suppliedEvent) {
      this.suppliedEvent = suppliedEvent;
    }

    private void setFailureBeforeSuccess(
        final RuntimeException exceptionToThrow, final int remainingFailureCount) {
      this.exceptionToThrow = exceptionToThrow;
      this.remainingFailureCount = remainingFailureCount;
    }

    @Override
    protected boolean consumeSkippedHistoricalTsFileEventIfNecessary(
        final TsFileResource resource) {
      if (!skippedTsFilePaths.contains(resource.getTsFilePath())) {
        return false;
      }
      consumedSkippedTsFilePaths.add(resource.getTsFilePath());
      return true;
    }

    @Override
    protected Event supplyTsFileEvent(final TsFileResource resource) {
      suppliedTsFiles.add(resource.getTsFilePath());
      if (remainingFailureCount > 0) {
        remainingFailureCount--;
        throw exceptionToThrow;
      }
      return suppliedEvent;
    }
  }
}
