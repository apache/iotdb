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
package org.apache.iotdb.db.conf.directories.strategy;

import java.util.List;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The basic class of all the strategies of multiple directories. If a user wants to define his own
 * strategy, his strategy has to extend this class and implement the abstract method.
 *
 * @author East
 */
public abstract class DirectoryStrategy {

  protected static final Logger logger = LoggerFactory.getLogger(DirectoryStrategy.class);

  /**
   * All the folders of data files, should be init once the subclass is created.
   */
  protected List<String> folders;

  /**
   * To init folders. Do not recommend to overwrite.
   * This method guarantees that at least one folder has available space.
   *
   * @param folders the folders from conf
   */
  public void init(List<String> folders) throws DiskSpaceInsufficientException {
    this.folders = folders;

    boolean hasSpace = false;
    for (String folder : folders) {
      if (CommonUtils.hasSpace(folder)) {
        hasSpace = true;
        break;
      }
    }
    if (!hasSpace) {
      throw new DiskSpaceInsufficientException(
          String.format("All disks of folders %s are full, can't init.", folders));
    }
  }

  /**
   * Choose a folder to allocate. The user should implement this method to define his own strategy.
   *
   * @return the index of folder that will be allocated
   */
  public abstract int nextFolderIndex() throws DiskSpaceInsufficientException;

  /**
   * Return the actual string value of a folder by its index.
   *
   * @param index the index of the folder
   * @return the string value of the folder
   */
  public String getTsFileFolder(int index) {
    return folders.get(index);
  }

  // only used by test
  public String getFolderForTest() {
    return getTsFileFolder(0);
  }

  // only used by test
  public void setFolderForTest(String path) {
    folders.set(0, path);
  }
}
