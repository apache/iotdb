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

  private static TsFileResource createTsFileResource(final File tempDir, final String fileName)
      throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());
    return new TsFileResource(file);
  }

  private static TsFileResource createClosedTsFileResource(
      final File tempDir, final String fileName, final ProgressIndex progressIndex)
      throws IOException {
    final TsFileResource resource = createTsFileResource(tempDir, fileName);
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
    Assert.assertEquals(!expected, startIndex.isEqualOrAfter(resourceProgressIndex));

    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    setPrivateField(source, "pipeName", "pipe");
    setPrivateField(source, "dataRegionId", 1);
    setPrivateField(source, "startIndex", startIndex);

    final Method method =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
            "mayTsFileContainUnprocessedData", TsFileResource.class);
    method.setAccessible(true);
    Assert.assertEquals(
        expected,
        method.invoke(
            source, createClosedTsFileResource(tempDir, fileName, resourceProgressIndex)));
  }

  private static ProgressIndex hybridProgressIndex(
      final ProgressIndex firstProgressIndex, final ProgressIndex... progressIndexes) {
    ProgressIndex result = new HybridProgressIndex(firstProgressIndex);
    for (final ProgressIndex progressIndex : progressIndexes) {
      result = result.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
    }
    return result;
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
