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
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimePartitionProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMetaVersion;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.resource.PersistentResource;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeHistoricalDataRegionTsFileAndDeletionSourceTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testActiveUnclosedTsFileIsSelectedConservatively() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalActiveTsFile").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_active_tsfile";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager resourceManager = PipeDataNodeResourceManager.tsfile();
    final TsFileManager tsFileManager =
        new TsFileManager("root.db", "2147483645", tempDir.getAbsolutePath());
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    try {
      final TsFileResource resource = createTsFileResource(partitionDir, "1-1-0-0.tsfile");
      resource.setTimeIndexType(ITimeIndex.FILE_TIME_INDEX_TYPE);
      resource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
      final TsFileProcessor processor = Mockito.mock(TsFileProcessor.class);
      Mockito.when(processor.alreadyMarkedClosing()).thenReturn(false);
      resource.setProcessor(processor);
      tsFileManager.add(resource, true);

      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);
      setPrivateField(source, "startIndex", new SimpleProgressIndex(0, 1));
      setPrivateField(source, "isHistoricalSourceEnabled", true);
      setPrivateField(source, "isModelDetected", true);
      setPrivateField(source, "isTableModel", true);
      setPrivateField(source, "tablePattern", new TablePattern(true, null, "selected_table"));

      final List<PersistentResource> extractedResources = new ArrayList<>();
      final Method extractTsFiles =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "extractTsFiles",
              TsFileManager.class,
              List.class,
              List.class,
              long.class,
              List.class);
      extractTsFiles.setAccessible(true);
      extractTsFiles.invoke(
          source,
          tsFileManager,
          tsFileManager.getTsFileList(true),
          Collections.<TsFileResource>emptyList(),
          System.currentTimeMillis(),
          extractedResources);

      final Map<TsFileResource, Set<String>> selectedResources =
          (Map<TsFileResource, Set<String>>)
              getPrivateField(source, "filteredTsFileResources2TableNames");
      Assert.assertEquals(Collections.singletonList(resource), extractedResources);
      Assert.assertTrue(selectedResources.containsKey(resource));
      Assert.assertNull(selectedResources.get(resource));

      final Method isCoveredByTablePattern =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "isTsFileResourceCoveredByTablePattern", TsFileResource.class, Set.class);
      isCoveredByTablePattern.setAccessible(true);
      Assert.assertFalse((Boolean) isCoveredByTablePattern.invoke(source, resource, null));
    } finally {
      source.close();
      tsFileManager.getTsFileList(true).forEach(resource -> tsFileManager.remove(resource, true));
      resourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testActiveUnclosedTsFilePinFailureFailsExtraction() throws Exception {
    final AtomicBoolean failPathResolution = new AtomicBoolean(true);
    final File tempDir = Files.createTempDirectory("pipeHistoricalActivePinFailure").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_active_pin_failure";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager resourceManager = PipeDataNodeResourceManager.tsfile();
    final TsFileManager tsFileManager =
        new TsFileManager("root.db", "2147483645", tempDir.getAbsolutePath());
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    try {
      final TsFileResource previouslyPinnedResource =
          createClosedTsFileResource(partitionDir, "1-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      final TsFileResource resource = createTsFileResource(partitionDir, "2-1-0-0.tsfile");
      final File previouslyPinnedTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              previouslyPinnedResource.getTsFile(), resourcePipeName);
      resource.setFile(new CanonicalPathFailingFile(resource.getTsFilePath(), failPathResolution));
      resource.setStatusForTest(TsFileResourceStatus.UNCLOSED);
      final TsFileProcessor processor = Mockito.mock(TsFileProcessor.class);
      Mockito.when(processor.alreadyMarkedClosing()).thenReturn(false);
      resource.setProcessor(processor);
      tsFileManager.add(previouslyPinnedResource, true);
      tsFileManager.add(resource, true);

      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);
      setPrivateField(source, "historicalDataExtractionStartTime", Long.MIN_VALUE);
      setPrivateField(source, "historicalDataExtractionEndTime", Long.MAX_VALUE);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);
      setPrivateField(source, "isHistoricalSourceEnabled", true);
      setPrivateField(source, "isDbNameCoveredByPattern", true);

      final Method extractTsFiles =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "extractTsFiles",
              TsFileManager.class,
              List.class,
              List.class,
              long.class,
              List.class);
      extractTsFiles.setAccessible(true);
      final InvocationTargetException extractionException =
          Assert.assertThrows(
              InvocationTargetException.class,
              () ->
                  extractTsFiles.invoke(
                      source,
                      tsFileManager,
                      Collections.singletonList(previouslyPinnedResource),
                      Collections.singletonList(resource),
                      System.currentTimeMillis(),
                      new ArrayList<PersistentResource>()));

      Assert.assertTrue(extractionException.getCause() instanceof PipeException);
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).isEmpty());
      Assert.assertEquals(
          0, resourceManager.getFileReferenceCount(previouslyPinnedTsFile, resourcePipeName));
    } finally {
      source.close();
      tsFileManager.getTsFileList(true).forEach(resource -> tsFileManager.remove(resource, true));
      resourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testClosingTsFileKeepsTimeFiltering() throws Exception {
    // A closing sequence file can temporarily look wholly inside the extraction window even
    // though the eventual file also contains rows outside it.
    assertSuppliedTsFileTimeFiltering(false, 0, Long.MIN_VALUE, false, true);
  }

  @Test
  public void testClosedTsFileTimeFiltering() throws Exception {
    assertSuppliedTsFileTimeFiltering(true, -10, 10, false, false);
    assertSuppliedTsFileTimeFiltering(true, -20, 20, false, true);
  }

  @Test
  public void testClosingTsFileHonorsLooseTimeRange() throws Exception {
    assertSuppliedTsFileTimeFiltering(false, 0, Long.MIN_VALUE, true, false);
  }

  @Test
  public void testHistoricalTsFileReferenceFailureRetriesWithoutAdvancingProgress()
      throws Exception {
    assertHistoricalTsFileReferenceFailure(true);
  }

  @Test
  public void testHistoricalTsFileReferenceFailureReleasesPinOnClose() throws Exception {
    assertHistoricalTsFileReferenceFailure(false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHistoricalExtractionRefreshesReplacedPartitionSnapshot() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalSnapshotRefresh").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_snapshot_refresh";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager pipeTsFileResourceManager =
        PipeDataNodeResourceManager.tsfile();
    final TsFileManager tsFileManager =
        new TsFileManager("root.db", "2147483645", tempDir.getAbsolutePath());
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    try {
      final TsFileResource staleResource =
          createClosedTsFileResource(partitionDir, "1-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      final TsFileResource currentResource =
          createClosedTsFileResource(partitionDir, "2-1-0-0.tsfile", new SimpleProgressIndex(0, 2));
      tsFileManager.add(staleResource, true);
      final List<TsFileResource> staleSnapshot = tsFileManager.getTsFileList(true);
      tsFileManager.remove(staleResource, true);
      tsFileManager.add(currentResource, true);

      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);
      setPrivateField(source, "isHistoricalSourceEnabled", true);
      setPrivateField(source, "isDbNameCoveredByPattern", true);

      final List<PersistentResource> originalResources = new ArrayList<>();
      final Method method =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "extractTsFiles",
              TsFileManager.class,
              List.class,
              List.class,
              long.class,
              List.class);
      method.setAccessible(true);
      method.invoke(
          source,
          tsFileManager,
          staleSnapshot,
          Collections.<TsFileResource>emptyList(),
          System.currentTimeMillis(),
          originalResources);

      final Map<TsFileResource, Set<String>> selectedResources =
          (Map<TsFileResource, Set<String>>)
              getPrivateField(source, "filteredTsFileResources2TableNames");
      Assert.assertEquals(Collections.singletonList(currentResource), originalResources);
      Assert.assertTrue(selectedResources.containsKey(currentResource));
      Assert.assertFalse(selectedResources.containsKey(staleResource));
      Assert.assertEquals(1, selectedResources.size());
      Assert.assertEquals(1, getPrivateField(source, "extractedHistoricalTsFileCount"));
    } finally {
      source.close();
      tsFileManager.getTsFileList(true).forEach(resource -> tsFileManager.remove(resource, true));
      pipeTsFileResourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalExtractionRefreshesPartitionReplacedDuringScan() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalConcurrentRefresh").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_concurrent_refresh";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager resourceManager = PipeDataNodeResourceManager.tsfile();
    final TsFileManager tsFileManager =
        new TsFileManager("root.db", "2147483645", tempDir.getAbsolutePath());
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    BlockingReadLockTsFileResource blockingResource = null;
    try {
      final TsFileResource firstStaleResource =
          createClosedTsFileResource(partitionDir, "1-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      final File blockingFile = new File(partitionDir, "2-1-0-0.tsfile");
      Assert.assertTrue(blockingFile.createNewFile());
      final CountDownLatch readLockEntered = new CountDownLatch(1);
      final CountDownLatch allowReadLock = new CountDownLatch(1);
      blockingResource =
          new BlockingReadLockTsFileResource(blockingFile, readLockEntered, allowReadLock);
      blockingResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      blockingResource.updateProgressIndex(new SimpleProgressIndex(0, 2));
      final TsFileResource currentResource =
          createClosedTsFileResource(partitionDir, "3-1-0-0.tsfile", new SimpleProgressIndex(0, 3));
      createExclusiveModFile(firstStaleResource);
      createExclusiveModFile(currentResource);

      tsFileManager.add(firstStaleResource, true);
      tsFileManager.add(blockingResource, true);
      final List<TsFileResource> staleSnapshot = tsFileManager.getTsFileList(true);
      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);
      setPrivateField(source, "isHistoricalSourceEnabled", true);
      setPrivateField(source, "isDbNameCoveredByPattern", true);
      setPrivateField(source, "shouldTransferModFile", true);

      final List<PersistentResource> originalResources = new ArrayList<>();
      final Method method =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "extractTsFiles",
              TsFileManager.class,
              List.class,
              List.class,
              long.class,
              List.class);
      method.setAccessible(true);
      final BlockingReadLockTsFileResource blockedResource = blockingResource;
      final Future<?> extraction =
          executor.submit(
              () -> {
                try {
                  method.invoke(
                      source,
                      tsFileManager,
                      staleSnapshot,
                      Collections.<TsFileResource>emptyList(),
                      System.currentTimeMillis(),
                      originalResources);
                } catch (final ReflectiveOperationException e) {
                  throw new RuntimeException(e);
                }
              });

      Assert.assertTrue(readLockEntered.await(10, TimeUnit.SECONDS));
      final Future<?> replacement =
          executor.submit(
              () -> {
                tsFileManager.remove(firstStaleResource, true);
                tsFileManager.remove(blockedResource, true);
                tsFileManager.add(currentResource, true);
              });
      replacement.get(10, TimeUnit.SECONDS);
      allowReadLock.countDown();
      extraction.get(10, TimeUnit.SECONDS);

      final File staleTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              firstStaleResource.getTsFile(), resourcePipeName);
      final File staleModFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              firstStaleResource.getExclusiveModFile().getFile(), resourcePipeName);
      final File currentTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              currentResource.getTsFile(), resourcePipeName);
      final File currentModFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              currentResource.getExclusiveModFile().getFile(), resourcePipeName);
      Assert.assertEquals(Collections.singletonList(currentResource), originalResources);
      Assert.assertEquals(0, resourceManager.getFileReferenceCount(staleTsFile, resourcePipeName));
      Assert.assertEquals(0, resourceManager.getFileReferenceCount(staleModFile, resourcePipeName));
      Assert.assertEquals(
          1, resourceManager.getFileReferenceCount(currentTsFile, resourcePipeName));
      Assert.assertEquals(
          1, resourceManager.getFileReferenceCount(currentModFile, resourcePipeName));
    } finally {
      if (blockingResource != null) {
        blockingResource.allowReadLock();
      }
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
      source.close();
      tsFileManager.getTsFileList(true).forEach(resource -> tsFileManager.remove(resource, true));
      resourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalModPinFailureRollsBackTsFilePin() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalModPinFailure").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_mod_pin_failure";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager resourceManager = PipeDataNodeResourceManager.tsfile();
    final TsFileManager tsFileManager =
        new TsFileManager("root.db", "2147483645", tempDir.getAbsolutePath());
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final AtomicBoolean failModPathResolution = new AtomicBoolean(true);
    try {
      final TsFileResource resource =
          createClosedTsFileResource(partitionDir, "1-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      final File modFile = resource.getExclusiveModFile().getFile();
      Files.write(modFile.toPath(), new byte[] {1});
      resource.setExclusiveModFile(
          new ModificationFile(
              new ModPathFailingFile(modFile.getPath(), failModPathResolution), false));
      tsFileManager.add(resource, true);

      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);
      setPrivateField(source, "isHistoricalSourceEnabled", true);
      setPrivateField(source, "isDbNameCoveredByPattern", true);
      setPrivateField(source, "shouldTransferModFile", true);
      final Method method =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "extractTsFiles",
              TsFileManager.class,
              List.class,
              List.class,
              long.class,
              List.class);
      method.setAccessible(true);
      method.invoke(
          source,
          tsFileManager,
          tsFileManager.getTsFileList(true),
          Collections.<TsFileResource>emptyList(),
          System.currentTimeMillis(),
          new ArrayList<PersistentResource>());

      final File pinnedTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getTsFile(), resourcePipeName);
      Assert.assertEquals(0, resourceManager.getFileReferenceCount(pinnedTsFile, resourcePipeName));
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).isEmpty());
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedModFile")).isEmpty());
    } finally {
      source.close();
      tsFileManager.getTsFileList(true).forEach(resource -> tsFileManager.remove(resource, true));
      resourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCloseRetriesTsFileUnpinAfterTsFileUnpinFailed() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileUnpinRetry").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_tsfile_unpin_retry";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager resourceManager = PipeDataNodeResourceManager.tsfile();
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final AtomicBoolean failTsFilePathResolution = new AtomicBoolean();
    try {
      final TsFileResource resource =
          createClosedTsFileResource(partitionDir, "1-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      resource.setFile(
          new CanonicalPathFailingFile(resource.getTsFilePath(), failTsFilePathResolution));
      createExclusiveModFile(resource);
      Assert.assertTrue(resourceManager.pinTsFileResource(resource, true, resourcePipeName));
      final File pinnedTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getTsFile(), resourcePipeName);
      final File pinnedModFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getExclusiveModFile().getFile(), resourcePipeName);
      ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).add(resource);
      ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedModFile")).add(resource);
      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);

      failTsFilePathResolution.set(true);
      final Method unpinMethod =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "unpinHistoricalTsFileResource", TsFileResource.class);
      unpinMethod.setAccessible(true);
      try {
        unpinMethod.invoke(source, resource);
        Assert.fail("Expected the TsFile path resolution to fail");
      } catch (final java.lang.reflect.InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof IOException);
      }

      Assert.assertEquals(1, resourceManager.getFileReferenceCount(pinnedTsFile, resourcePipeName));
      Assert.assertEquals(
          0, resourceManager.getFileReferenceCount(pinnedModFile, resourcePipeName));
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile"))
              .contains(resource));
      Assert.assertFalse(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedModFile"))
              .contains(resource));

      failTsFilePathResolution.set(false);
      source.close();
      Assert.assertEquals(0, resourceManager.getFileReferenceCount(pinnedTsFile, resourcePipeName));
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).isEmpty());
    } finally {
      failTsFilePathResolution.set(false);
      source.close();
      resourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCloseRetriesModUnpinAfterTsFileUnpinSucceeded() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalModUnpinRetry").toFile();
    final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
    Assert.assertTrue(partitionDir.mkdirs());
    final String pipeName = "historical_mod_unpin_retry";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, System.nanoTime());
    final PipeTsFileResourceManager resourceManager = PipeDataNodeResourceManager.tsfile();
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final AtomicBoolean failModPathResolution = new AtomicBoolean();
    try {
      final TsFileResource resource =
          createClosedTsFileResource(partitionDir, "1-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      final File modFile = resource.getExclusiveModFile().getFile();
      Files.write(modFile.toPath(), new byte[] {1});
      resource.setExclusiveModFile(
          new ModificationFile(
              new ModPathFailingFile(modFile.getPath(), failModPathResolution), false));
      Assert.assertTrue(resourceManager.pinTsFileResource(resource, true, resourcePipeName));
      final File pinnedTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getTsFile(), resourcePipeName);
      final File pinnedModFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getExclusiveModFile().getFile(), resourcePipeName);
      ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).add(resource);
      ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedModFile")).add(resource);
      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "dataRegionId", 2147483645);

      failModPathResolution.set(true);
      final Method unpinMethod =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "unpinHistoricalTsFileResource", TsFileResource.class);
      unpinMethod.setAccessible(true);
      try {
        unpinMethod.invoke(source, resource);
        Assert.fail("Expected the mod path resolution to fail");
      } catch (final java.lang.reflect.InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof IOException);
      }

      Assert.assertEquals(0, resourceManager.getFileReferenceCount(pinnedTsFile, resourcePipeName));
      Assert.assertEquals(
          1, resourceManager.getFileReferenceCount(pinnedModFile, resourcePipeName));
      Assert.assertFalse(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile"))
              .contains(resource));
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedModFile"))
              .contains(resource));

      failModPathResolution.set(false);
      source.close();
      Assert.assertEquals(
          0, resourceManager.getFileReferenceCount(pinnedModFile, resourcePipeName));
      Assert.assertTrue(
          ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedModFile")).isEmpty());
    } finally {
      failModPathResolution.set(false);
      source.close();
      resourceManager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @SuppressWarnings("unchecked")
  private void assertHistoricalTsFileReferenceFailure(final boolean retry) throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalReferenceFailure").toFile();
    final String pipeName = "historical_reference_retry";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, 1L);
    final PipeTsFileResourceManager manager = PipeDataNodeResourceManager.tsfile();
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final AtomicBoolean failPathResolution = new AtomicBoolean();
    PipeTsFileInsertionEvent event = null;
    try {
      final File partitionDir = new File(tempDir, "sequence/db/2147483645/0");
      Assert.assertTrue(partitionDir.mkdirs());
      final TsFileResource resource = createTsFileResource(partitionDir, "1-1-0-0.tsfile");
      resource.setFile(new CanonicalPathFailingFile(resource.getTsFilePath(), failPathResolution));
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      resource.updateProgressIndex(new SimpleProgressIndex(0, 1));

      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "creationTime", 1L);
      setPrivateField(source, "dataRegionId", 2147483645);
      setPrivateField(source, "hasBeenStarted", true);
      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority", true);
      setPrivateField(source, "isModelDetected", true);
      setPrivateField(source, "isTableModel", true);
      setPrivateField(source, "tablePattern", new TablePattern(true, null, null));
      setPrivateField(source, "isDbNameCoveredByPattern", true);
      final Map<TsFileResource, Set<String>> selectedResources =
          (Map<TsFileResource, Set<String>>)
              getPrivateField(source, "filteredTsFileResources2TableNames");
      selectedResources.put(resource, Collections.emptySet());
      final Set<PersistentResource> progressReportResources =
          (Set<PersistentResource>) getPrivateField(source, "historicalProgressReportResources");
      progressReportResources.add(resource);
      setPrivateField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(Collections.singletonList(resource)));

      manager.pinTsFileResource(resource, false, resourcePipeName);
      ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).add(resource);
      final File pinnedFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getTsFile(), resourcePipeName);
      failPathResolution.set(true);
      Assert.assertThrows(PipeException.class, source::supply);
      Assert.assertEquals(1, source.getPendingQueueSize());
      Assert.assertTrue(selectedResources.containsKey(resource));
      Assert.assertTrue(progressReportResources.contains(resource));
      Assert.assertNull(getPrivateField(source, "pendingHistoricalProgressIndexToReport"));
      Assert.assertEquals(1, manager.getFileReferenceCount(pinnedFile, resourcePipeName));

      failPathResolution.set(false);
      if (retry) {
        event = (PipeTsFileInsertionEvent) source.supply();
        Assert.assertNotNull(event);
        final Field resourceField = PipeTsFileInsertionEvent.class.getDeclaredField("resource");
        resourceField.setAccessible(true);
        Assert.assertSame(resource, resourceField.get(event));
        Assert.assertEquals(0, source.getPendingQueueSize());
        Assert.assertFalse(selectedResources.containsKey(resource));
        // The event now owns the only remaining reference; the source has released its pin.
        Assert.assertEquals(1, manager.getFileReferenceCount(pinnedFile, resourcePipeName));
        final Event progressEvent = source.supply();
        Assert.assertTrue(progressEvent instanceof ProgressReportEvent);
        Assert.assertEquals(
            new TimePartitionProgressIndex(
                resource.getTimePartition(), resource.getMaxProgressIndex()),
            ((ProgressReportEvent) progressEvent).getProgressIndex());
      } else {
        source.close();
        Assert.assertEquals(0, manager.getFileReferenceCount(pinnedFile, resourcePipeName));
      }
    } finally {
      failPathResolution.set(false);
      if (event != null) {
        event.clearReferenceCount(getClass().getName());
      }
      source.close();
      manager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @SuppressWarnings("unchecked")
  private void assertSuppliedTsFileTimeFiltering(
      final boolean closed,
      final long fileStartTime,
      final long fileEndTime,
      final boolean looseTimeRange,
      final boolean shouldParseTime)
      throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalClosingTimeFilter").toFile();
    final String pipeName = "closing_time_filter";
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, 1L);
    final PipeTsFileResourceManager manager = PipeDataNodeResourceManager.tsfile();
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    TsFileResource resource = null;
    PipeTsFileInsertionEvent event = null;
    boolean pinned = false;
    try {
      final File partitionDir = new File(tempDir, "sequence/db/2147483646/0");
      Assert.assertTrue(partitionDir.mkdirs());
      resource = createTsFileResource(partitionDir, "1-1-0-0.tsfile");
      final IDeviceID device =
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table1", "device1"});
      resource.updateStartTime(device, fileStartTime);
      resource.updateEndTime(device, fileEndTime);
      resource.setStatusForTest(
          closed ? TsFileResourceStatus.NORMAL : TsFileResourceStatus.UNCLOSED);
      manager.pinTsFileResource(resource, false, resourcePipeName);
      pinned = true;

      setPrivateField(source, "pipeName", pipeName);
      setPrivateField(source, "pipeNameWithCreationTime", resourcePipeName);
      setPrivateField(source, "creationTime", 1L);
      setPrivateField(source, "dataRegionId", 2147483646);
      setPrivateField(source, "isModelDetected", true);
      setPrivateField(source, "isTableModel", true);
      setPrivateField(source, "tablePattern", new TablePattern(true, null, null));
      setPrivateField(source, "isDbNameCoveredByPattern", true);
      setPrivateField(source, "historicalDataExtractionStartTime", -10L);
      setPrivateField(source, "historicalDataExtractionEndTime", 10L);
      setPrivateField(source, "sloppyTimeRange", looseTimeRange);
      ((Set<TsFileResource>) getPrivateField(source, "resourcesWithPinnedTsFile")).add(resource);
      ((Map<TsFileResource, Set<String>>)
              getPrivateField(source, "filteredTsFileResources2TableNames"))
          .put(resource, Collections.emptySet());

      event = (PipeTsFileInsertionEvent) source.supplyTsFileEvent(resource);
      pinned = false;
      Assert.assertNotNull(event);
      // The collector must not send this entire file without filtering after its close completes.
      Assert.assertEquals(shouldParseTime, event.shouldParseTime());
      Assert.assertEquals(!shouldParseTime, PipeEventCollector.canSkipParsing4TsFileEvent(event));
      if (!closed && !looseTimeRange) {
        resource.updateStartTime(device, -20);
        resource.updateEndTime(device, 20);
        resource.setStatusForTest(TsFileResourceStatus.NORMAL);
        Assert.assertTrue(event.shouldParseTime());
        Assert.assertFalse(PipeEventCollector.canSkipParsing4TsFileEvent(event));
      }
    } finally {
      if (event != null) {
        event.clearReferenceCount(getClass().getName());
      }
      if (pinned) {
        manager.unpinTsFileResource(resource, resourcePipeName, false);
      }
      source.close();
      manager.cleanPipeTsFileDir(resourcePipeName);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

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
  public void testMissingTsFileResourceDoesNotBlockHistoricalExtraction() throws Exception {
    final PipeHistoricalDataRegionTsFileAndDeletionSource source =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalMissingResource").toFile();

    try {
      final TsFileResource resource = createTsFileResource(tempDir, "missing-resource.tsfile");
      resource.setTimeIndex(new FileTimeIndex());
      setPrivateField(source, "pipeName", "pipe");
      setPrivateField(source, "dataRegionId", 1);
      setPrivateField(source, "treePattern", new PrefixTreePattern("root.**"));

      final Method method =
          PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredMethod(
              "mayTsFileResourceOverlappedWithPattern", TsFileResource.class);
      method.setAccessible(true);

      Assert.assertTrue((Boolean) method.invoke(source, resource));
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
  public void testGlobalTimeRangeRespectsHistoryEnable() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_START_TIME_KEY, "1000");
    attributes.put(PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY, Boolean.FALSE.toString());

    final PipeHistoricalDataRegionTsFileAndDeletionSource realtimeOnlySource =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    realtimeOnlySource.validate(
        new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));

    Assert.assertFalse((Boolean) getPrivateField(realtimeOnlySource, "isHistoricalSourceEnabled"));
    Assert.assertEquals(
        1000L,
        ((Long) getPrivateField(realtimeOnlySource, "historicalDataExtractionStartTime"))
            .longValue());

    final PipeHistoricalDataRegionTsFileAndDeletionSource defaultSource =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    attributes.remove(PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY);
    defaultSource.validate(
        new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));

    Assert.assertTrue((Boolean) getPrivateField(defaultSource, "isHistoricalSourceEnabled"));

    final PipeHistoricalDataRegionTsFileAndDeletionSource restartedSource =
        new PipeHistoricalDataRegionTsFileAndDeletionSource();
    attributes.put(PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY, Boolean.FALSE.toString());
    attributes.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.TRUE.toString());
    restartedSource.validate(
        new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));

    Assert.assertTrue((Boolean) getPrivateField(restartedSource, "isHistoricalSourceEnabled"));
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
  public void testRatisFilesWithoutPipeProgressAreNotCoveredByCheckpoint() throws Exception {
    final String originalConsensus =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    final File tempDir = Files.createTempDirectory("pipeHistoricalRatisCoverage").toFile();
    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "unknown-initial.tsfile",
          MinimumProgressIndex.INSTANCE,
          MinimumProgressIndex.INSTANCE,
          true);
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "unknown-recovery.tsfile",
          new SimpleProgressIndex(0, 100),
          MinimumProgressIndex.INSTANCE,
          true);
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "indexed-covered.tsfile",
          new SimpleProgressIndex(0, 100),
          new SimpleProgressIndex(0, 50),
          false);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "other-consensus.tsfile",
          MinimumProgressIndex.INSTANCE,
          MinimumProgressIndex.INSTANCE,
          false);
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(originalConsensus);
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

  @Test
  public void testFilterAndSortTsFilesUsingRestoredWindowStateProgressIndex() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalWindowStateOrder").toFile();

    try {
      final TimeWindowStateProgressIndex windowStateProgressIndex =
          new TimeWindowStateProgressIndex(
              Map.of(
                  "root.sg.d.completed", new Pair<>(100L, null),
                  "root.sg.d.open", new Pair<>(200L, ByteBuffer.wrap(new byte[] {1}))));
      final ProgressIndex wrappedProgressIndex =
          new StateProgressIndex(
              1L,
              Collections.emptyMap(),
              hybridProgressIndex(windowStateProgressIndex, new SimpleProgressIndex(0, 10)));
      final PipeTaskMeta restoredTaskMeta =
          roundTripTaskMeta(new PipeTaskMeta(wrappedProgressIndex, 1));

      final TsFileResource beforeWatermark =
          createClosedTsFileResource(tempDir, "50-1-0-0.tsfile", new SimpleProgressIndex(0, 11));
      final TsFileResource atWatermark =
          createClosedTsFileResource(tempDir, "90-1-0-0.tsfile", new SimpleProgressIndex(0, 2));
      final TsFileResource afterWatermark =
          createClosedTsFileResource(tempDir, "95-1-0-0.tsfile", new SimpleProgressIndex(0, 1));
      final IDeviceID device =
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"root.sg", "d"});
      beforeWatermark.updateStartTime(device, 50L);
      beforeWatermark.updateEndTime(device, 99L);
      atWatermark.updateStartTime(device, 90L);
      atWatermark.updateEndTime(device, 100L);
      afterWatermark.updateStartTime(device, 95L);
      afterWatermark.updateEndTime(device, 101L);

      final PipeHistoricalDataRegionTsFileAndDeletionSource source =
          new PipeHistoricalDataRegionTsFileAndDeletionSource();
      final PipeParameters parameters =
          new PipeParameters(
              Map.of(
                  PipeSourceConstant.SOURCE_INCLUSION_KEY,
                  "data.insert",
                  PipeSourceConstant.SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                  "false"));
      source.validate(new PipeParameterValidator(parameters));
      source.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  PipeStaticMeta.CONSENSUS_PIPE_PREFIX + "test", 1, 123, restoredTaskMeta)));
      final ProgressIndex restoredStartIndex =
          (ProgressIndex) getPrivateField(source, "startIndex");
      Assert.assertNotNull(
          restoredStartIndex
              .getProgressIndexByType(TimeWindowStateProgressIndex.class)
              .orElse(null));

      assertMayTsFileContainUnprocessedData(source, beforeWatermark, false);
      assertMayTsFileContainUnprocessedData(source, atWatermark, true);
      assertMayTsFileContainUnprocessedData(source, afterWatermark, true);
      final List<PersistentResource> resources =
          new ArrayList<>(Arrays.asList(afterWatermark, atWatermark));
      sortExtractedResources(source, resources);

      Assert.assertEquals(Arrays.asList(atWatermark, afterWatermark), resources);
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

  private static void createExclusiveModFile(final TsFileResource resource) throws IOException {
    final File modFile = resource.getExclusiveModFile().getFile();
    Files.write(modFile.toPath(), new byte[] {1});
    resource.setExclusiveModFile(new ModificationFile(modFile, false));
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
    assertMayTsFileContainUnprocessedData(source, resource, expected);
  }

  private static void assertMayTsFileContainUnprocessedData(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source,
      final TsFileResource resource,
      final boolean expected)
      throws ReflectiveOperationException {
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

  private static PipeTaskMeta roundTripTaskMeta(final PipeTaskMeta taskMeta) throws IOException {
    try (final PublicBAOS output = new PublicBAOS()) {
      taskMeta.serialize(output);
      return PipeTaskMeta.deserialize(
          PipeRuntimeMetaVersion.VERSION_2, ByteBuffer.wrap(output.getBuf(), 0, output.size()));
    }
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

  private static class CanonicalPathFailingFile extends File {

    private final AtomicBoolean failPathResolution;

    private CanonicalPathFailingFile(final String path, final AtomicBoolean failPathResolution) {
      super(path);
      this.failPathResolution = failPathResolution;
    }

    @Override
    public File getParentFile() {
      final File parent = super.getParentFile();
      return parent == null
          ? null
          : new CanonicalPathFailingFile(parent.getPath(), failPathResolution);
    }

    @Override
    public String getCanonicalPath() throws IOException {
      if (failPathResolution.get()) {
        throw new IOException("mock path resolution failure");
      }
      return super.getCanonicalPath();
    }
  }

  private static class ModPathFailingFile extends File {

    private final AtomicBoolean failPathResolution;

    private ModPathFailingFile(final String path, final AtomicBoolean failPathResolution) {
      super(path);
      this.failPathResolution = failPathResolution;
    }

    @Override
    public String getCanonicalPath() throws IOException {
      if (failPathResolution.get()) {
        throw new IOException("mock mod path resolution failure");
      }
      return super.getCanonicalPath();
    }
  }

  private static class BlockingReadLockTsFileResource extends TsFileResource {

    private final CountDownLatch readLockEntered;
    private final CountDownLatch allowReadLock;
    private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

    private BlockingReadLockTsFileResource(
        final File file, final CountDownLatch readLockEntered, final CountDownLatch allowReadLock) {
      super(file);
      this.readLockEntered = readLockEntered;
      this.allowReadLock = allowReadLock;
    }

    @Override
    public void readLock() {
      if (shouldBlock.compareAndSet(true, false)) {
        readLockEntered.countDown();
        try {
          if (!allowReadLock.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting to acquire the test read lock");
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      super.readLock();
    }

    private void allowReadLock() {
      allowReadLock.countDown();
    }
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
