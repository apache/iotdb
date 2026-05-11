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

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.resource.PersistentResource;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

  private static TsFileResource createTsFileResource(final File tempDir, final String fileName)
      throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());
    return new TsFileResource(file);
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
      return suppliedEvent;
    }
  }
}
