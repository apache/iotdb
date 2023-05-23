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
package org.apache.iotdb.db.conf.directories;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.conf.directories.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.db.conf.directories.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.db.conf.directories.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.OBJECT_STORAGE_DIR;

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
  /** seq file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> seqDir2TierLevel = new HashMap<>();
  /** unSeq file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> unSeqDir2TierLevel = new HashMap<>();
  /** total space of each tier, Long.MAX_VALUE when one tier contains remote storage */
  private long[] tierDiskTotalSpace;

  private TierManager() {
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
    resetFolders();
  }

  public void resetFolders() {
    if (config.getDataNodeId() == -1) {
      return;
    }

    seqTiers.clear();
    unSeqTiers.clear();
    seqDir2TierLevel.clear();
    unSeqDir2TierLevel.clear();

    String[][] tierDirs = config.getTierDataDirs();
    for (int i = 0; i < tierDirs.length; ++i) {
      for (int j = 0; j < tierDirs[i].length; ++j) {
        if (tierDirs[i][j].equals(OBJECT_STORAGE_DIR)) {
          tierDirs[i][j] =
              FSUtils.getOSDefaultPath(config.getObjectStorageBucket(), config.getDataNodeId());
        } else if (FSUtils.isLocal(tierDirs[i][j])) {
          try {
            tierDirs[i][j] = new File(tierDirs[i][j]).getCanonicalPath();
          } catch (IOException e) {
            logger.error("Fail to get canonical path of data dir {}", tierDirs[i][j], e);
          }
        }
      }
    }

    for (int tierLevel = 0; tierLevel < tierDirs.length; ++tierLevel) {
      List<String> seqDirs =
          Arrays.stream(tierDirs[tierLevel])
              .map(
                  v ->
                      FSFactoryProducer.getFSFactory()
                          .getFile(v, IoTDBConstant.SEQUENCE_FLODER_NAME)
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
              .map(
                  v ->
                      FSFactoryProducer.getFSFactory()
                          .getFile(v, IoTDBConstant.UNSEQUENCE_FLODER_NAME)
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
    }

    tierDiskTotalSpace = getTierDiskSpace(DiskSpaceType.TOTAL);
  }

  private void mkDataDirs(List<String> folders) {
    for (String folder : folders) {
      File file = FSFactoryProducer.getFSFactory().getFile(folder);
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

  public int getTiersNum() {
    return seqTiers.size();
  }

  public int getFileTierLevel(File file) {
    if (!file.exists()) {
      return getTiersNum() - 1;
    }

    String filePath;
    try {
      filePath = file.getCanonicalPath();
    } catch (IOException e) {
      logger.error("Fail to get canonical path of data dir {}", file, e);
      filePath = file.getPath();
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
    throw new RuntimeException(String.format("%s is not a legal TsFile path", file));
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
        // get the FileStore of each local dir
        Path path = Paths.get(dir);
        FileStore fileStore = null;
        try {
          fileStore = Files.getFileStore(path);
        } catch (IOException e) {
          // check parent if path is not exists
          path = path.getParent();
          try {
            fileStore = Files.getFileStore(path);
          } catch (IOException innerException) {
            logger.error("Failed to get storage path of {}, because", dir, innerException);
          }
        }
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
