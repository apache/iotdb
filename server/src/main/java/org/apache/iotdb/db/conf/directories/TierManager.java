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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  /** seq file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> seqDir2TierLevel = new HashMap<>();
  /** unSeq file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> unSeqDir2TierLevel = new HashMap<>();

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
    String[][] tierDirs = config.getTierDataDirs();
    for (int tierLevel = 0; tierLevel < tierDirs.length; ++tierLevel) {
      List<String> seqDirs =
          Arrays.stream(tierDirs[tierLevel])
              .map(v -> v + File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME)
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
              .map(v -> v + File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME)
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

  public String getNextFolderForSequenceFile(int tierLevel) throws DiskSpaceInsufficientException {
    return seqTiers.get(tierLevel).getNextFolder();
  }

  public String getNextFolderForUnSequenceFile(int tierLevel)
      throws DiskSpaceInsufficientException {
    return unSeqTiers.get(tierLevel).getNextFolder();
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

  public static TierManager getInstance() {
    return TierManagerHolder.INSTANCE;
  }

  private static class TierManagerHolder {

    private static final TierManager INSTANCE = new TierManager();
  }
}
