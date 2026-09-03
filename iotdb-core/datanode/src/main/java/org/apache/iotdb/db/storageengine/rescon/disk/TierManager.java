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
package org.apache.iotdb.db.storageengine.rescon.disk;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.service.metrics.DataNodeExceptionMetrics;
import org.apache.iotdb.metrics.utils.FileStoreUtils;

import com.google.common.io.BaseEncoding;
import org.apache.ratis.util.MemoizedCheckedSupplier;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** The main class of multiple directories. Used to allocate folders to data files. */
public class TierManager {
  private static final Logger logger = LoggerFactory.getLogger(TierManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private DirectoryStrategyType directoryStrategyType = DirectoryStrategyType.SEQUENCE_STRATEGY;

  /**
   * seq folder manager of each storage tier, managing both data directories and multi-dir strategy
   */
  private volatile List<FolderManager> seqTiers = new ArrayList<>();

  /**
   * unSeq folder manager of each storage tier, managing both data directories and multi-dir
   * strategy
   */
  private volatile List<FolderManager> unSeqTiers = new ArrayList<>();

  private volatile List<FolderManager> objectTiers = new ArrayList<>();

  /** seq file folder's rawFsPath path -> tier level */
  private volatile Map<String, Integer> seqDir2TierLevel = new HashMap<>();

  /** unSeq file folder's rawFsPath path -> tier level */
  private volatile Map<String, Integer> unSeqDir2TierLevel = new HashMap<>();

  /**
   * Object folders across all tiers, ordered from hot (tier 0) to cold. Used for cross-tier lookup
   * and full-directory scans (delete / snapshot / disk usage).
   */
  private volatile List<String> objectDirs = new ArrayList<>();

  /** object folder's path -> tier level */
  private volatile Map<String, Integer> objectDir2TierLevel = new HashMap<>();

  private List<String> copyToTargetDirs;

  //  private FolderManager copyToFolderManager;

  private MemoizedCheckedSupplier<FolderManager, DiskSpaceInsufficientException>
      copyToFolderManager;

  /** total space of each tier, Long.MAX_VALUE when one tier contains remote storage */
  private long[] tierDiskTotalSpace;

  private TierManager() {
    initFolders();
  }

  public synchronized void initFolders() {
    initFolders(seqTiers, unSeqTiers, objectTiers, seqDir2TierLevel, unSeqDir2TierLevel);
  }

  private void initFolders(
      List<FolderManager> seqTiers,
      List<FolderManager> unSeqTiers,
      List<FolderManager> objectTiers,
      Map<String, Integer> seqDir2TierLevel,
      Map<String, Integer> unSeqDir2TierLevel) {
    directoryStrategyType =
        DirectoryStrategyType.fromClassName(config.getMultiDirStrategyClassName());

    config.updatePath();
    String[][] tierDirs = config.getTierDataDirs();
    List<String> allObjectDirs = new ArrayList<>();
    Map<String, Integer> allObjectDir2TierLevel = new HashMap<>();
    for (int i = 0; i < tierDirs.length; ++i) {
      for (int j = 0; j < tierDirs[i].length; ++j) {
        switch (FSUtils.getFSType(tierDirs[i][j])) {
          case LOCAL:
            try {
              tierDirs[i][j] = new File(tierDirs[i][j]).getCanonicalPath();
            } catch (IOException e) {
              logger.error(StorageEngineMessages.FAIL_TO_GET_CANONICAL_PATH, tierDirs[i][j], e);
              DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
            }
            break;
          case OBJECT_STORAGE:
            if (i != tierDirs.length - 1 || tierDirs.length == 1) {
              File defaultDataDir =
                  new File(
                      IoTDBConstant.DN_DEFAULT_DATA_DIR
                          + File.separator
                          + IoTDBConstant.DATA_FOLDER_NAME);
              logger.error(
                  "Object Storage can only exist on the last tier and have local dirs in the front tiers, use default data dir {} instaed.",
                  defaultDataDir);
              try {
                tierDirs[i][j] = defaultDataDir.getCanonicalPath();
              } catch (IOException e) {
                logger.error("Fail to get canonical path of data dir {}", tierDirs[i][j], e);
              }
            } else {
              // reset datanode id
              tierDirs[i][j] =
                  FSUtils.getOSDefaultPath(config.getObjectStorageBucket(), config.getDataNodeId());
            }
            break;
          case HDFS:
          default:
            break;
        }
      }
    }
    config.setTierDataDirs(tierDirs);

    for (int tierLevel = 0; tierLevel < tierDirs.length; ++tierLevel) {
      List<String> seqDirs =
          Arrays.stream(tierDirs[tierLevel])
              .filter(Objects::nonNull)
              .map(
                  v ->
                      FSFactoryProducer.getFSFactory()
                          .getFile(v, IoTDBConstant.SEQUENCE_FOLDER_NAME)
                          .getPath())
              .collect(Collectors.toList());
      mkDataDirs(seqDirs);
      try {
        seqTiers.add(new FolderManager(seqDirs, directoryStrategyType));
      } catch (DiskSpaceInsufficientException e) {
        logger.error(StorageEngineMessages.ALL_DISKS_OF_TIER_FULL, tierLevel, e);
      }
      for (String dir : seqDirs) {
        seqDir2TierLevel.put(dir, tierLevel);
      }

      List<String> unSeqDirs =
          Arrays.stream(tierDirs[tierLevel])
              .filter(Objects::nonNull)
              .map(
                  v ->
                      FSFactoryProducer.getFSFactory()
                          .getFile(v, IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
                          .getPath())
              .collect(Collectors.toList());
      mkDataDirs(unSeqDirs);
      try {
        unSeqTiers.add(new FolderManager(unSeqDirs, directoryStrategyType));
      } catch (DiskSpaceInsufficientException e) {
        logger.error(StorageEngineMessages.ALL_DISKS_OF_TIER_FULL, tierLevel, e);
      }
      for (String dir : unSeqDirs) {
        unSeqDir2TierLevel.put(dir, tierLevel);
      }

      if (tierLevel == 0) {
        copyToTargetDirs =
            Arrays.stream(tierDirs[tierLevel])
                .filter(Objects::nonNull)
                .map(
                    v ->
                        FSFactoryProducer.getFSFactory()
                            .getFile(v, IoTDBConstant.COPY_TO_TARGET_FOLDER_NAME)
                            .getPath())
                .collect(Collectors.toList());
        copyToFolderManager =
            MemoizedCheckedSupplier.valueOf(
                () -> new FolderManager(copyToTargetDirs, directoryStrategyType));
      }

      List<String> tierObjectDirs =
          Arrays.stream(tierDirs[tierLevel])
              .filter(Objects::nonNull)
              .map(
                  v ->
                      FSFactoryProducer.getFSFactory()
                          .getFile(v, IoTDBConstant.OBJECT_FOLDER_NAME)
                          .getPath())
              .collect(Collectors.toList());

      try {
        objectTiers.add(new FolderManager(tierObjectDirs, directoryStrategyType));
      } catch (DiskSpaceInsufficientException e) {
        logger.error(StorageEngineMessages.ALL_DISKS_OF_TIER_FULL, tierLevel, e);
      }
      // try to remove empty objectDirs
      for (String dir : tierObjectDirs) {
        File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);
        if (dirFile.isDirectory() && Objects.requireNonNull(dirFile.list()).length == 0) {
          try {
            Files.delete(dirFile.toPath());
          } catch (IOException ignore) {
          }
        }
        allObjectDir2TierLevel.put(dir, tierLevel);
      }
      allObjectDirs.addAll(tierObjectDirs);
    }

    this.objectDirs = allObjectDirs;
    this.objectDir2TierLevel = allObjectDir2TierLevel;
    tierDiskTotalSpace = getTierDiskSpace(DiskSpaceType.TOTAL);
  }

  public synchronized void resetFolders() {
    long startTime = System.currentTimeMillis();
    List<FolderManager> newSeqTiers = new ArrayList<>();
    List<FolderManager> newUnSeqTiers = new ArrayList<>();
    List<FolderManager> newObjectTiers = new ArrayList<>();
    Map<String, Integer> newSeqDir2TierLevel = new HashMap<>();
    Map<String, Integer> newUnSeqDir2TierLevel = new HashMap<>();

    initFolders(
        newSeqTiers, newUnSeqTiers, newObjectTiers, newSeqDir2TierLevel, newUnSeqDir2TierLevel);
    seqTiers = newSeqTiers;
    unSeqTiers = newUnSeqTiers;
    objectTiers = newObjectTiers;
    seqDir2TierLevel = newSeqDir2TierLevel;
    unSeqDir2TierLevel = newUnSeqDir2TierLevel;
    long endTime = System.currentTimeMillis();
    logger.info(StorageEngineMessages.FOLDERS_RESET_SUCCESSFULLY, (endTime - startTime));
  }

  private void mkDataDirs(List<String> folders) {
    for (String folder : folders) {
      File file = FSFactoryProducer.getFSFactory().getFile(folder);
      if (FSUtils.getFSType(folder) == FSType.OBJECT_STORAGE) {
        continue;
      }
      if (file.mkdirs()) {
        logger.info(StorageEngineMessages.FOLDER_NOT_EXIST_CREATE_IT, file.getPath());
      } else {
        logger.info(
            StorageEngineMessages.STORAGE_LOG_CREATE_FOLDER_FAILED_IS_THE_FOLDER_EXISTED_18E29D51,
            file.getPath(),
            file.exists());
      }
    }
  }

  public String getNextFolderForTsFile(int tierLevel, boolean sequence)
      throws DiskSpaceInsufficientException {
    return sequence
        ? seqTiers.get(tierLevel).getNextFolder()
        : unSeqTiers.get(tierLevel).getNextFolder();
  }

  public String getNextFolderForCopyToTargetFile() throws DiskSpaceInsufficientException {
    return copyToFolderManager.get().getNextFolder();
  }

  public String getNextFolderForObjectFile() throws DiskSpaceInsufficientException {
    return getNextFolderForObjectFile(0);
  }

  /** Allocate an object folder on the given tier (0 = hot). */
  public String getNextFolderForObjectFile(int tierLevel) throws DiskSpaceInsufficientException {
    return objectTiers.get(tierLevel).getNextFolder();
  }

  public FolderManager getFolderManager(int tierLevel, boolean sequence) {
    return sequence ? seqTiers.get(tierLevel) : unSeqTiers.get(tierLevel);
  }

  public List<String> getAllFilesFolders() {
    List<String> folders = new ArrayList<>(seqDir2TierLevel.keySet());
    folders.addAll(unSeqDir2TierLevel.keySet());
    return folders;
  }

  public List<String> getAllLocalFilesFolders() {
    return getAllFilesFolders().stream().filter(FSUtils::isLocal).collect(Collectors.toList());
  }

  public List<String> getAllSequenceFileFolders() {
    return new ArrayList<>(seqDir2TierLevel.keySet());
  }

  public List<String> getAllLocalSequenceFileFolders() {
    return seqDir2TierLevel.keySet().stream().filter(FSUtils::isLocal).collect(Collectors.toList());
  }

  public List<String> getAllUnSequenceFileFolders() {
    return new ArrayList<>(unSeqDir2TierLevel.keySet());
  }

  public List<String> getAllLocalUnSequenceFileFolders() {
    return unSeqDir2TierLevel.keySet().stream()
        .filter(FSUtils::isLocal)
        .collect(Collectors.toList());
  }

  public List<String> getAllObjectFileFolders() {
    return new ArrayList<>(objectDirs);
  }

  /** Object folders belonging to the given tier (may be multiple disks). */
  public List<String> getObjectFoldersForTier(int tierLevel) {
    List<String> folders = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : objectDir2TierLevel.entrySet()) {
      if (entry.getValue() == tierLevel) {
        folders.add(entry.getKey());
      }
    }
    return folders;
  }

  /**
   * Resolve which tier an object file / object folder path belongs to. Falls back to 0 if unmatched
   * (same default as {@link #getFileTierLevel(File)}).
   */
  public int getObjectFileTierLevel(File file) {
    Path filePath;
    try {
      filePath = file.getCanonicalFile().toPath();
    } catch (IOException e) {
      logger.error(StorageEngineMessages.FAIL_TO_GET_CANONICAL_PATH, file, e);
      filePath = file.toPath();
    }
    for (Map.Entry<String, Integer> entry : objectDir2TierLevel.entrySet()) {
      Path objectDirPath;
      try {
        objectDirPath = new File(entry.getKey()).getCanonicalFile().toPath();
      } catch (IOException e) {
        objectDirPath = new File(entry.getKey()).toPath();
      }
      if (filePath.startsWith(objectDirPath)) {
        return entry.getValue();
      }
    }
    return 0;
  }

  public boolean isDataRegionObjectDirExists(String dataRegionId) {
    for (String objectDir : objectDirs) {
      File dataRegionDir = FSFactoryProducer.getFSFactory().getFile(objectDir, dataRegionId);
      if (dataRegionDir.exists()) {
        return true;
      }
    }
    return false;
  }

  public Optional<File> getAbsoluteObjectFilePath(String filePath) {
    return getAbsoluteObjectFilePath(filePath, false);
  }

  /**
   * Resolve an object file by relative path. {@code objectDirs} is hot-to-cold, so a local copy is
   * preferred; {@code OBJECT_STORAGE} is checked after a local miss.
   */
  public Optional<File> getAbsoluteObjectFilePath(String filePath, boolean needTempFile) {
    for (String objectDir : objectDirs) {
      File objectFile = FSFactoryProducer.getFSFactory().getFile(objectDir, filePath);
      if (objectFile.exists()) {
        return Optional.of(objectFile);
      }
      if (needTempFile) {
        File tmpFile = FSFactoryProducer.getFSFactory().getFile(objectDir, filePath + ".tmp");
        File backFile = FSFactoryProducer.getFSFactory().getFile(objectDir, filePath + ".back");
        if (tmpFile.exists() || backFile.exists()) {
          return Optional.of(objectFile);
        }
      }
    }
    return Optional.empty();
  }

  public List<File> getAllMatchedObjectDirs(String regionIdStr, String... path) {
    List<File> matchedDirs = new ArrayList<>();
    StringBuilder objectPath = new StringBuilder();
    objectPath.append(regionIdStr);
    for (String str : path) {
      objectPath
          .append(File.separator)
          .append(
              CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit()
                  ? str
                  : BaseEncoding.base32()
                      .omitPadding()
                      .encode(str.getBytes(StandardCharsets.UTF_8)));
    }
    for (String objectDir : objectDirs) {
      File objectFilePath =
          FSFactoryProducer.getFSFactory().getFile(objectDir, objectPath.toString());
      // OBJECT_STORAGE has no directory objects: exists() is false for a prefix, but DROP/SCAN
      // must still visit that prefix (deleteObjectsByPrefix / list by suffix).
      if (FSUtils.getFSType(objectFilePath) != FSType.LOCAL || objectFilePath.exists()) {
        matchedDirs.add(objectFilePath);
      }
    }
    return matchedDirs;
  }

  public int getTiersNum() {
    return seqTiers.size();
  }

  public int getFileTierLevel(File file) {
    Path filePath;
    try {
      filePath = file.getCanonicalFile().toPath();
    } catch (IOException e) {
      logger.error(StorageEngineMessages.FAIL_TO_GET_CANONICAL_PATH, file, e);
      DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
      filePath = file.toPath();
    }

    for (Map.Entry<String, Integer> entry : seqDir2TierLevel.entrySet()) {
      if (filePath.startsWith(entry.getKey())) {
        return entry.getValue();
      }
    }
    for (Map.Entry<String, Integer> entry : unSeqDir2TierLevel.entrySet()) {
      if (filePath.startsWith(entry.getKey())) {
        return entry.getValue();
      }
    }
    return 0;
  }

  public long[] getTierDiskTotalSpace() {
    return Arrays.copyOf(tierDiskTotalSpace, tierDiskTotalSpace.length);
  }

  public long[] getTierDiskUsableSpace() {
    return getTierDiskSpace(DiskSpaceType.USABLE);
  }

  private long[] getTierDiskSpace(DiskSpaceType type) {
    String[][] tierDirs = config.getTierDataDirs();
    long[] tierDiskSpace = new long[tierDirs.length];
    for (int tierLevel = 0; tierLevel < tierDirs.length; ++tierLevel) {
      Set<FileStore> tierFileStores = new HashSet<>();
      for (String dir : tierDirs[tierLevel]) {
        if (!FSUtils.isLocal(dir)) {
          tierDiskSpace[tierLevel] = Long.MAX_VALUE;
          break;
        }
        FileStore fileStore = FileStoreUtils.getFileStore(dir);
        // update space info
        if (fileStore != null && !tierFileStores.contains(fileStore)) {
          tierFileStores.add(fileStore);
          try {
            switch (type) {
              case TOTAL:
                tierDiskSpace[tierLevel] += fileStore.getTotalSpace();
                break;
              case USABLE:
                tierDiskSpace[tierLevel] += fileStore.getUsableSpace();
                break;
              default:
                break;
            }
          } catch (IOException e) {
            logger.error(StorageEngineMessages.FAILED_TO_STATISTIC_SIZE, fileStore, e);
            DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
          }
        }
      }
    }
    return tierDiskSpace;
  }

  private enum DiskSpaceType {
    TOTAL,
    USABLE,
  }

  public static TierManager getInstance() {
    return TierManagerHolder.INSTANCE;
  }

  private static class TierManagerHolder {

    private static final TierManager INSTANCE = new TierManager();
  }
}
