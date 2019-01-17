/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.conf.directories;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class of multiple directories. Used to allocate folders to data files.
 *
 * @author East
 */
public class Directories {

  private static final Logger LOGGER = LoggerFactory.getLogger(Directories.class);

  private List<String> tsfileFolders;
  private DirectoryStrategy strategy;

  private Directories() {
    tsfileFolders = new ArrayList<String>(
        Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getBufferWriteDirs()));
    initFolders();

    String strategyName = "";
    try {
      strategyName = IoTDBDescriptor.getInstance().getConfig().multDirStrategyClassName;
      Class<?> clazz = Class.forName(strategyName);
      strategy = (DirectoryStrategy) clazz.newInstance();
      strategy.init(tsfileFolders);
    } catch (Exception e) {
      LOGGER.error("can't find strategy {} for mult-directories.", strategyName);
    }
  }

  public static Directories getInstance() {
    return DirectoriesHolder.INSTANCE;
  }

  private void initFolders() {
    for (String folder : tsfileFolders) {
      File file = new File(folder);
      if (file.mkdirs()) {
        LOGGER.info("folder {} in tsfileFolders doesn't exist, create it", file.getPath());
      }
    }
  }

  // only used by test
  public String getFolderForTest() {
    return tsfileFolders.get(0);
  }

  // only used by test
  public void setFolderForTest(String path) {
    tsfileFolders.set(0, path);
  }

  public String getNextFolderForTsfile() {
    return getTsFileFolder(getNextFolderIndexForTsFile());
  }

  /**
   * get next folder index for TsFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForTsFile() {
    int index = 0;
    index = strategy.nextFolderIndex();
    return index;
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

    private static final Directories INSTANCE = new Directories();
  }
}
