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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.consensus.ProgressIndexDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.DeletionBuffer;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.PageCacheDeletionBuffer;
import org.apache.iotdb.db.pipe.consensus.deletion.recover.DeletionReader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DeletionResourceManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionResourceManager.class);
  public static final String DELETION_FILE_SUFFIX = ".deletion";
  public static final String MAGIC_VERSION_V1 = "DELETION_V1";
  private static final String REBOOT_TIME = "rebootTime";
  private static final String MEM_TABLE_FLUSH_ORDER = "memTableFlushOrderId";
  private static final String DELETION_FILE_NAME_PATTERN =
      String.format(
          "^_(?<%s>\\d+)-(?<%s>\\d+)\\%s$",
          REBOOT_TIME, MEM_TABLE_FLUSH_ORDER, DELETION_FILE_SUFFIX);
  private final String dataRegionId;
  private final DeletionBuffer deletionBuffer;
  private final File storageDir;
  private final Map<AbstractDeleteDataNode, DeletionResource> deleteNode2ResourcesMap =
      new ConcurrentHashMap<>();
  private final Lock recoverLock = new ReentrantLock();
  private final Condition recoveryReadyCondition = recoverLock.newCondition();
  private volatile boolean hasCompletedRecovery = false;

  private DeletionResourceManager(String dataRegionId) throws IOException {
    this.dataRegionId = dataRegionId;
    this.storageDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir()
                + File.separator
                + dataRegionId);
    this.deletionBuffer = new PageCacheDeletionBuffer(dataRegionId, storageDir.getAbsolutePath());
    initAndRecover();
    // Only after initAndRecover can we start serialize and sync new deletions.
    this.deletionBuffer.start();
  }

  private void initAndRecover() throws IOException {
    recoverLock.lock();
    try {
      if (!storageDir.exists()) {
        // Init
        if (!storageDir.mkdirs()) {
          LOGGER.warn("Unable to create pipeConsensus deletion dir at {}", storageDir);
          throw new IOException(
              String.format("Unable to create pipeConsensus deletion dir at %s", storageDir));
        }
      }
      try (Stream<Path> pathStream = Files.walk(Paths.get(storageDir.getPath()), 1)) {
        Path[] deletionPaths =
            pathStream
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().matches(DELETION_FILE_NAME_PATTERN))
                .toArray(Path[]::new);

        for (Path path : deletionPaths) {
          try (DeletionReader deletionReader =
              new DeletionReader(path.toFile(), dataRegionId, this::removeDeletionResource)) {
            deletionReader
                .readAllDeletions()
                .forEach(
                    deletion ->
                        deleteNode2ResourcesMap.computeIfAbsent(
                            deletion.getDeleteDataNode(), key -> deletion));
          } catch (IOException e) {
            LOGGER.warn(
                "Detect file corrupted when recover DAL-{}, discard all subsequent DALs...",
                path.getFileName());
            break;
          }
        }
        hasCompletedRecovery = true;
        recoveryReadyCondition.signalAll();
      }
    } finally {
      recoverLock.unlock();
    }
  }

  @Override
  public void close() {
    LOGGER.info("Closing deletion resource manager for {}...", dataRegionId);
    this.deleteNode2ResourcesMap.clear();
    this.deletionBuffer.close();
    LOGGER.info("Deletion resource manager for {} has been successfully closed!", dataRegionId);
  }

  public DeletionResource registerDeletionResource(AbstractDeleteDataNode deleteDataNode) {
    DeletionResource deletionResource =
        deleteNode2ResourcesMap.computeIfAbsent(
            deleteDataNode,
            key ->
                new DeletionResource(deleteDataNode, this::removeDeletionResource, dataRegionId));
    // register a persist task for current deletionResource
    deletionBuffer.registerDeletionResource(deletionResource);
    return deletionResource;
  }

  public DeletionResource getDeletionResource(AbstractDeleteDataNode deleteDataNode) {
    return deleteNode2ResourcesMap.get(deleteDataNode);
  }

  public List<DeletionResource> getAllDeletionResources() {
    recoverLock.lock();
    try {
      if (!hasCompletedRecovery) {
        recoveryReadyCondition.await();
      }
      return deleteNode2ResourcesMap.values().stream().collect(ImmutableList.toImmutableList());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "DeletionManager-{}: current waiting is interrupted. May because current application is down. ",
          dataRegionId,
          e);
      return deleteNode2ResourcesMap.values().stream().collect(ImmutableList.toImmutableList());
    } finally {
      recoverLock.unlock();
    }
  }

  /**
   * This is a hook function, which will be automatically invoked when deletionResource's reference
   * count returns to 0.
   */
  private synchronized void removeDeletionResource(DeletionResource deletionResource) {
    // Clean memory
    deleteNode2ResourcesMap.remove(deletionResource.getDeleteDataNode());
    // Clean disk
    ProgressIndex currentProgressIndex =
        ProgressIndexDataNodeManager.extractLocalSimpleProgressIndex(
            deletionResource.getProgressIndex());

    try (Stream<Path> pathStream = Files.walk(Paths.get(storageDir.getPath()), 1)) {
      Path[] deletionPaths =
          pathStream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().matches(DELETION_FILE_NAME_PATTERN))
              .filter(
                  path ->
                      isFileProgressCoveredByGivenProgress(
                          path.getFileName().toString(), currentProgressIndex))
              .sorted(this::compareFileProgressIndex)
              .toArray(Path[]::new);
      // File name represents the max progressIndex in its previous file. If currentProgressIndex is
      // larger than a fileName's progressIndex, it means that the file before this file has been
      // fully synchronized and can be deleted.
      // So here we cannot guarantee that the last file can be deleted, we can only guarantee that
      // the first n-1 files can be deleted (if the length of deletionPaths is n)
      for (int i = 0; i < deletionPaths.length - 1; i++) {
        File fileToDelete = deletionPaths[i].toFile();
        FileUtils.deleteFileOrDirectory(fileToDelete);
        LOGGER.info(
            "DeletionManager-{} delete deletion file in {} dir...", dataRegionId, fileToDelete);
      }
    } catch (IOException e) {
      LOGGER.warn(
          "DeletionManager-{} failed to delete file in {} dir, please manually check!",
          dataRegionId,
          storageDir);
    }
  }

  private int compareFileProgressIndex(Path file1, Path file2) {
    Pattern pattern = Pattern.compile(DELETION_FILE_NAME_PATTERN);
    String fileName1 = file1.getFileName().toString();
    String fileName2 = file2.getFileName().toString();
    Matcher matcher1 = pattern.matcher(fileName1);
    Matcher matcher2 = pattern.matcher(fileName2);
    // Definitely match. Because upper caller has filtered fileNames.
    if (matcher1.matches() && matcher2.matches()) {
      int fileRebootTimes1 = Integer.parseInt(matcher1.group(REBOOT_TIME));
      long fileMemTableFlushOrderId1 = Long.parseLong(matcher1.group(MEM_TABLE_FLUSH_ORDER));

      int fileRebootTimes2 = Integer.parseInt(matcher2.group(REBOOT_TIME));
      long fileMemTableFlushOrderId2 = Long.parseLong(matcher2.group(MEM_TABLE_FLUSH_ORDER));

      int rebootCompareRes = Integer.compare(fileRebootTimes1, fileRebootTimes2);
      return rebootCompareRes == 0
          ? Long.compare(fileMemTableFlushOrderId1, fileMemTableFlushOrderId2)
          : rebootCompareRes;
    }
    return 0;
  }

  private boolean isFileProgressCoveredByGivenProgress(
      String fileName, ProgressIndex currentProgressIndex) {
    if (currentProgressIndex instanceof SimpleProgressIndex) {
      SimpleProgressIndex simpleProgressIndex = (SimpleProgressIndex) currentProgressIndex;
      int curRebootTimes = simpleProgressIndex.getRebootTimes();
      long curMemTableFlushOrderId = simpleProgressIndex.getMemTableFlushOrderId();

      Pattern pattern = Pattern.compile(DELETION_FILE_NAME_PATTERN);
      Matcher matcher = pattern.matcher(fileName);
      // Definitely match. Because upper caller has filtered fileNames.
      if (matcher.matches()) {
        int fileRebootTimes = Integer.parseInt(matcher.group(REBOOT_TIME));
        long fileMemTableFlushOrderId = Long.parseLong(matcher.group(MEM_TABLE_FLUSH_ORDER));
        return fileRebootTimes == curRebootTimes
            ? fileMemTableFlushOrderId <= curMemTableFlushOrderId
            : fileRebootTimes < curRebootTimes;
      }
    }
    return false;
  }

  //////////////////////////// singleton ////////////////////////////
  private static class DeletionResourceManagerHolder {
    private static Map<String, DeletionResourceManager> CONSENSU_GROUP_ID_2_INSTANCE_MAP;

    private DeletionResourceManagerHolder() {}

    public static void build() {
      if (CONSENSU_GROUP_ID_2_INSTANCE_MAP == null) {
        CONSENSU_GROUP_ID_2_INSTANCE_MAP = new ConcurrentHashMap<>();
      }
    }
  }

  public static DeletionResourceManager getInstance(String groupId) {
    // If consensusImpl is not PipeConsensus.
    if (DeletionResourceManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP == null) {
      return null;
    }
    return DeletionResourceManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP.computeIfAbsent(
        groupId,
        key -> {
          try {
            return new DeletionResourceManager(groupId);
          } catch (IOException e) {
            LOGGER.error("Failed to initialize DeletionResourceManager", e);
            throw new RuntimeException(e);
          }
        });
  }

  // Only when consensus protocol is PipeConsensus, will this class be initialized.
  public static void build() {
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus) {
      DeletionResourceManagerHolder.build();
    }
  }

  public static void exit() {
    if (DeletionResourceManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP == null) {
      return;
    }
    DeletionResourceManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP.forEach(
        (groupId, resourceManager) -> {
          resourceManager.close();
        });
  }

  @TestOnly
  public static void buildForTest() {
    DeletionResourceManagerHolder.build();
  }

  @TestOnly
  public void recoverForTest() {
    try (Stream<Path> pathStream = Files.walk(Paths.get(storageDir.getPath()), 1)) {
      Path[] deletionPaths =
          pathStream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().matches(DELETION_FILE_NAME_PATTERN))
              .toArray(Path[]::new);

      for (Path path : deletionPaths) {
        try (DeletionReader deletionReader =
            new DeletionReader(path.toFile(), dataRegionId, this::removeDeletionResource)) {
          deletionReader
              .readAllDeletions()
              .forEach(
                  deletion ->
                      deleteNode2ResourcesMap.computeIfAbsent(
                          deletion.getDeleteDataNode(), key -> deletion));
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to recover DeletionResourceManager", e);
    }
  }
}
