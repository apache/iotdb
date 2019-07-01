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
 *
 * @author East
 */
public class DirectoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryManager.class);

  private List<String> tsfileFolders;
  private List<String> overflowFolders;
  private DirectoryStrategy tsfileStrategy;
  private DirectoryStrategy overflowStrategy;

  private DirectoryManager() {
    tsfileFolders = new ArrayList<>(
        Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getBufferWriteDirs()));
    initFolders(tsfileFolders);
    overflowFolders = new ArrayList<>(
    Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getOverflowDataDirs()));
    initFolders(overflowFolders);

    String strategyName = "";
    try {
      strategyName = IoTDBDescriptor.getInstance().getConfig().getMultDirStrategyClassName();
      Class<?> clazz = Class.forName(strategyName);
      tsfileStrategy = (DirectoryStrategy) clazz.newInstance();
      tsfileStrategy.init(tsfileFolders);
      overflowStrategy = (DirectoryStrategy) clazz.newInstance();
      overflowStrategy.init(overflowFolders);
    } catch (Exception e) {
      LOGGER.error("can't find tsfileStrategy {} for mult-directories.", strategyName, e);
    }
  }

  public static DirectoryManager getInstance() {
    return DirectoriesHolder.INSTANCE;
  }

  private void initFolders(List<String> folders) {
    for (String folder : folders) {
      File file = new File(folder);
      if (file.mkdirs()) {
        LOGGER.info("folder {} doesn't exist, create it", file.getPath());
      }
    }
  }

  // only used by test
  public String getTsFolderForTest() {
    return tsfileFolders.get(0);
  }

  // only used by test
  public void setTsFolderForTest(String path) {
    tsfileFolders.set(0, path);
  }

  public String getNextFolderForSequenceFile() throws DiskSpaceInsufficientException {
    return getTsFileFolder(getNextFolderIndexForTsFile());
  }

  /**
   * get next folder index for TsFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForTsFile() throws DiskSpaceInsufficientException {
    return tsfileStrategy.nextFolderIndex();
  }

  public String getTsFileFolder(int index) {
    return tsfileFolders.get(index);
  }

  public int getTsFileFolderIndex(String folder) {
    return tsfileFolders.indexOf(folder);
  }

  public List<String> getAllTsFileFolders() {
    return tsfileFolders;
  }

  private static class DirectoriesHolder {
    private static final DirectoryManager INSTANCE = new DirectoryManager();
  }

  public String getWALFolder() {
    return IoTDBDescriptor.getInstance().getConfig().getWalFolder();
  }

  public String getNextFolderForUnSequenceFile() throws DiskSpaceInsufficientException {
    return getOverflowFileFolder(getNextFolderIndexForOverflowFile());
  }

  /**
   * get next folder index for OverflowFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForOverflowFile() throws DiskSpaceInsufficientException {
    return overflowStrategy.nextFolderIndex();
  }

  public String getOverflowFileFolder(int index) {
    return overflowFolders.get(index);
  }

  public int getOverflowFileFolderIndex(String folder) {
    return overflowFolders.indexOf(folder);
  }

  public List<String> getAllOverflowFileFolders() {
    return overflowFolders;
  }

  // only used by test
  public String getOverflowFolderForTest() {
    return overflowFolders.get(0);
  }
}
