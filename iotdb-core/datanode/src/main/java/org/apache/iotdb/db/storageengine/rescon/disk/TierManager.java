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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.metrics.utils.FileStoreUtils;

import com.google.common.io.BaseEncoding;
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
  private final List<FolderManager> seqTiers = new ArrayList<>();

  /**
   * unSeq folder manager of each storage tier, managing both data directories and multi-dir
   * strategy
   */
  private final List<FolderManager> unSeqTiers = new ArrayList<>();

  private final List<FolderManager> objectTiers = new ArrayList<>();

  /** seq file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> seqDir2TierLevel = new HashMap<>();

  /** unSeq file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> unSeqDir2TierLevel = new HashMap<>();

  private List<String> objectDirs;

  /** total space of each tier, Long.MAX_VALUE when one tier contains remote storage */
  private long[] tierDiskTotalSpace;

  private TierManager() {
    initFolders();
  }

  public synchronized void initFolders() {
    try {
      String strategyName = Class.forName(config.getMultiDirStrategyClassName()).getSimpleName();
      if (strategyName.equals(MaxDiskUsableSpaceFirstStrategy.class.getSimpleName())) {
        directoryStrategyType = DirectoryStrategyType.MAX_DISK_USABLE_SPACE_FIRST_STRATEGY;
      } else if (strategyName.equals(MinFolderOccupiedSpaceFirstStrategy.class.getSimpleName())) {
        directoryStrategyType = DirectoryStrategyType.MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY;
      } else if (strategyName.equals(RandomOnDiskUsableSpaceStrategy.class.getSimpleName())) {
        directoryStrategyType = DirectoryStrategyType.RANDOM_ON_DISK_USABLE_SPACE_STRATEGY;
      }
    } catch (Exception e) {
      logger.error(
          "Can't find strategy {} for mult-directories.", config.getMultiDirStrategyClassName(), e);
    }

    config.updatePath();
    String[][] tierDirs = config.getTierDataDirs();
    for (int i = 0; i < tierDirs.length; ++i) {
      for (int j = 0; j < tierDirs[i].length; ++j) {
        switch (FSUtils.getFSType(tierDirs[i][j])) {
          case LOCAL:
            try {
              tierDirs[i][j] = new File(tierDirs[i][j]).getCanonicalPath();
            } catch (IOException e) {
              logger.error("Fail to get canonical path of data dir {}", tierDirs[i][j], e);
            }
            break;
          case OBJECT_STORAGE:
          case HDFS:
          default:
            break;
        }
      }
    }

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
        logger.error("All disks of tier {} are full.", tierLevel, e);
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
        logger.error("All disks of tier {} are full.", tierLevel, e);
      }
      for (String dir : unSeqDirs) {
        unSeqDir2TierLevel.put(dir, tierLevel);
      }

      objectDirs =
          Arrays.stream(tierDirs[tierLevel])
              .filter(Objects::nonNull)
              .map(
                  v ->
                      FSFactoryProducer.getFSFactory()
                          .getFile(v, IoTDBConstant.OBJECT_FOLDER_NAME)
                          .getPath())
              .collect(Collectors.toList());

      try {
        objectTiers.add(new FolderManager(objectDirs, directoryStrategyType));
      } catch (DiskSpaceInsufficientException e) {
        logger.error("All disks of tier {} are full.", tierLevel, e);
      }
      // try to remove empty objectDirs
      for (String dir : objectDirs) {
        File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);
        if (dirFile.isDirectory() && Objects.requireNonNull(dirFile.list()).length == 0) {
          try {
            Files.delete(dirFile.toPath());
          } catch (IOException ignore) {
          }
        }
      }
    }

    tierDiskTotalSpace = getTierDiskSpace(DiskSpaceType.TOTAL);
  }

  public synchronized void resetFolders() {
    long startTime = System.currentTimeMillis();
    seqTiers.clear();
    unSeqTiers.clear();
    objectTiers.clear();
    seqDir2TierLevel.clear();
    unSeqDir2TierLevel.clear();

    initFolders();
    long endTime = System.currentTimeMillis();
    logger.info("The folders is reset successfully, which takes {} ms.", (endTime - startTime));
  }

  private void mkDataDirs(List<String> folders) {
    for (String folder : folders) {
      File file = FSFactoryProducer.getFSFactory().getFile(folder);
      if (FSUtils.getFSType(folder) == FSType.OBJECT_STORAGE) {
        continue;
      }
      if (file.mkdirs()) {
        logger.info("folder {} doesn't exist, create it", file.getPath());
      } else {
        logger.info(
            "create folder {} failed. Is the folder existed: {}", file.getPath(), file.exists());
      }
    }
  }

  public String getNextFolderForTsFile(int tierLevel, boolean sequence)
      throws DiskSpaceInsufficientException {
    return sequence
        ? seqTiers.get(tierLevel).getNextFolder()
        : unSeqTiers.get(tierLevel).getNextFolder();
  }

  public String getNextFolderForObjectFile() throws DiskSpaceInsufficientException {
    return objectTiers.get(0).getNextFolder();
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
    return objectDirs;
  }

  public Optional<File> getAbsoluteObjectFilePath(String filePath) {
    return getAbsoluteObjectFilePath(filePath, false);
  }

  public Optional<File> getAbsoluteObjectFilePath(String filePath, boolean needTempFile) {
    for (String objectDir : objectDirs) {
      File objectFile = FSFactoryProducer.getFSFactory().getFile(objectDir, filePath);
      if (objectFile.exists()) {
        return Optional.of(objectFile);
      }
      if (needTempFile) {
        if (new File(objectFile.getPath() + ".tmp").exists()
            || new File(objectFile.getPath() + ".back").exists()) {
          return Optional.of(objectFile);
        }
      }
    }
    return Optional.empty();
  }

  public List<File> getAllMatchedObjectDirs(String regionIdStr, String... path) {
    List<File> matchedDirs = new ArrayList<>();
    boolean hasObjectDir = false;
    for (String objectDir : objectDirs) {
      File objectDirPath = FSFactoryProducer.getFSFactory().getFile(objectDir);
      if (objectDirPath.exists()) {
        hasObjectDir = true;
        break;
      }
    }
    if (!hasObjectDir) {
      return matchedDirs;
    }
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
      if (objectFilePath.exists()) {
        matchedDirs.add(objectFilePath);
      }
    }
    return matchedDirs;
  }

  public int getTiersNum() {
    return seqTiers.size();
  }

  public int getFileTierLevel(File file) {
    // If the file does not exist on Local disk, it is assumed be on remote Object Storage
    if (!file.exists()) {
      return getTiersNum() - 1;
    }
    Path filePath;
    try {
      filePath = file.getCanonicalFile().toPath();
    } catch (IOException e) {
      logger.error("Fail to get canonical path of data dir {}", file, e);
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
            logger.error("Failed to statistic the size of {}, because", fileStore, e);
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
