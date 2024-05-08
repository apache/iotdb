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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** The main class of multiple directories. Used to allocate folders to data files. */
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
      sequenceFileFolders.set(
          i, sequenceFileFolders.get(i) + File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME);
    }
    mkDataDirs(sequenceFileFolders);

    unsequenceFileFolders =
        new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    for (int i = 0; i < unsequenceFileFolders.size(); i++) {
      unsequenceFileFolders.set(
          i, unsequenceFileFolders.get(i) + File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME);
    }
    mkDataDirs(unsequenceFileFolders);

    String strategyName = "";
    try {
      strategyName = IoTDBDescriptor.getInstance().getConfig().getMultiDirStrategyClassName();
      Class<?> clazz = Class.forName(strategyName);
      sequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      sequenceStrategy.setFolders(sequenceFileFolders);
      unsequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      unsequenceStrategy.setFolders(unsequenceFileFolders);
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All disks of folders are full.", e);
    } catch (Exception e) {
      logger.error("Can't find strategy {} for mult-directories.", strategyName, e);
    }
  }

  public void updateFileFolders() throws LoadConfigurationException {
    try {
      List<String> sequenceFileFolders =
          new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
      for (int i = 0; i < sequenceFileFolders.size(); i++) {
        sequenceFileFolders.set(
            i, sequenceFileFolders.get(i) + File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME);
      }
      mkDataDirs(sequenceFileFolders);

      List<String> unsequenceFileFolders =
          new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
      for (int i = 0; i < unsequenceFileFolders.size(); i++) {
        unsequenceFileFolders.set(
            i,
            unsequenceFileFolders.get(i) + File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME);
      }
      mkDataDirs(unsequenceFileFolders);
      sequenceStrategy.setFolders(sequenceFileFolders);
      unsequenceStrategy.setFolders(unsequenceFileFolders);
      this.sequenceFileFolders = sequenceFileFolders;
      this.unsequenceFileFolders = unsequenceFileFolders;
      logger.info("Success to update file folders.");
    } catch (DiskSpaceInsufficientException e) {
      logger.error("Fail to update file folders, use previous folders.", e);
      throw new LoadConfigurationException(
          "Fail to update file folders because all disks of folders are full, use previous folders.");
    }
  }

  public void updateDirectoryStrategy() throws LoadConfigurationException {
    String strategyName = "";
    try {
      strategyName = IoTDBDescriptor.getInstance().getConfig().getMultiDirStrategyClassName();
      Class<?> clazz = Class.forName(strategyName);
      sequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      sequenceStrategy.setFolders(sequenceFileFolders);
      unsequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      unsequenceStrategy.setFolders(unsequenceFileFolders);
      logger.info("Success to update directory strategy.");
    } catch (Exception e) {
      logger.error("Fail to update directory strategy {}, use previous strategy", strategyName, e);
      throw new LoadConfigurationException(
          String.format(
              "Fail to update directory strategy because can't find strategy %s for mult-directories, use previous strategy",
              strategyName));
    }
  }

  public static DirectoryManager getInstance() {
    return DirectoriesHolder.INSTANCE;
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

  public String getNextFolderForSequenceFile() throws DiskSpaceInsufficientException {
    try {
      return sequenceFileFolders.get(sequenceStrategy.nextFolderIndex());
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All disks of wal folders are full, change system mode to read-only.", e);
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      throw e;
    }
  }

  public List<String> getAllSequenceFileFolders() {
    return new ArrayList<>(sequenceFileFolders);
  }

  public String getNextFolderForUnSequenceFile() throws DiskSpaceInsufficientException {
    try {
      return unsequenceFileFolders.get(unsequenceStrategy.nextFolderIndex());
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All disks of wal folders are full, change system mode to read-only.", e);
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
      throw e;
    }
  }

  public List<String> getAllUnSequenceFileFolders() {
    return new ArrayList<>(unsequenceFileFolders);
  }

  public List<String> getAllFilesFolders() {
    List<String> folders = new ArrayList<>(sequenceFileFolders);
    folders.addAll(unsequenceFileFolders);
    return folders;
  }

  @TestOnly
  public void resetFolders() {
    sequenceFileFolders =
        new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    for (int i = 0; i < sequenceFileFolders.size(); i++) {
      sequenceFileFolders.set(
          i, sequenceFileFolders.get(i) + File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME);
    }
    unsequenceFileFolders =
        new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    for (int i = 0; i < unsequenceFileFolders.size(); i++) {
      unsequenceFileFolders.set(
          i, unsequenceFileFolders.get(i) + File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME);
    }
  }

  private static class DirectoriesHolder {
    private static final DirectoryManager INSTANCE = new DirectoryManager();
  }
}
