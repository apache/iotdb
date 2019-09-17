/**
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class of multiple directories. Used to allocate folders to data files.
 */
public class DirectoryManager {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryManager.class);

  private List<String> sequenceFileFolders;
  private List<String> unsequenceFileFolders;
  private DirectoryStrategy sequenceStrategy;
  private DirectoryStrategy unsequenceStrategy;

  private DirectoryManager() {
    sequenceFileFolders =
        new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    for (int i = 0; i < sequenceFileFolders.size(); i++) {
      sequenceFileFolders.set(i, sequenceFileFolders.get(i) + File.separator + "sequence");
    }
    mkDirs(sequenceFileFolders);

    unsequenceFileFolders =
        new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    for (int i = 0; i < unsequenceFileFolders.size(); i++) {
      unsequenceFileFolders.set(i, unsequenceFileFolders.get(i) + File.separator + "unsequence");
    }
    mkDirs(unsequenceFileFolders);

    String strategyName = "";
    try {
      strategyName = IoTDBDescriptor.getInstance().getConfig().getMultiDirStrategyClassName();
      Class<?> clazz = Class.forName(strategyName);
      sequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      sequenceStrategy.init(sequenceFileFolders);
      unsequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      unsequenceStrategy.init(unsequenceFileFolders);
    } catch (Exception e) {
      logger.error("can't find sequenceStrategy {} for mult-directories.", strategyName, e);
    }
  }

  public static DirectoryManager getInstance() {
    return DirectoriesHolder.INSTANCE;
  }

  private void mkDirs(List<String> folders) {
    for (String folder : folders) {
      File file = new File(folder);
      if (file.mkdirs()) {
        logger.info("folder {} doesn't exist, create it", file.getPath());
      } else {
        logger.info("create folder {} failed. Is the folder existed: {}", file.getPath(),
            file.exists());
      }
    }
  }
  
  public String getNextFolderForSequenceFile() throws DiskSpaceInsufficientException {
    return getSequenceFileFolder(getNextFolderIndexForSequenceFile());
  }

  /**
   * get next folder index for TsFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForSequenceFile() throws DiskSpaceInsufficientException {
    return sequenceStrategy.nextFolderIndex();
  }

  public String getSequenceFileFolder(int index) {
    return sequenceFileFolders.get(index);
  }

  public int getSequenceFileFolderIndex(String folder) {
    return sequenceFileFolders.indexOf(folder);
  }

  public List<String> getAllSequenceFileFolders() {
    return sequenceFileFolders;
  }

  private static class DirectoriesHolder {
    private static final DirectoryManager INSTANCE = new DirectoryManager();
  }

  public String getWALFolder() {
    return IoTDBDescriptor.getInstance().getConfig().getWalFolder();
  }

  public String getNextFolderForUnSequenceFile() throws DiskSpaceInsufficientException {
    return getUnSequenceFileFolder(getNextFolderIndexForUnSequenceFile());
  }

  /**
   * get next folder index for OverflowFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForUnSequenceFile() throws DiskSpaceInsufficientException {
    return unsequenceStrategy.nextFolderIndex();
  }

  public String getUnSequenceFileFolder(int index) {
    return unsequenceFileFolders.get(index);
  }

  public int getUnSequenceFileFolderIndex(String folder) {
    return unsequenceFileFolders.indexOf(folder);
  }

  public List<String> getAllUnSequenceFileFolders() {
    return unsequenceFileFolders;
  }

  // only used by test
  public String getUnSequenceFolderForTest() {
    return unsequenceFileFolders.get(0);
  }
}
