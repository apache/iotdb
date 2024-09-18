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
package org.apache.iotdb.db.storageengine.rescon.disk.strategy;

import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;

import org.apache.tsfile.fileSystem.FSFactoryProducer;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SequenceStrategy extends DirectoryStrategy {

  private static final long PRINT_INTERVAL_MS = 3600 * 1000L;
  private int currentIndex;
  private Map<Integer, Long> dirLastPrintTimeMap = new ConcurrentHashMap<>();

  @Override
  public void setFolders(List<String> folders) throws DiskSpaceInsufficientException {
    super.setFolders(folders);

    // super.setFolders() ensures at least one folder is not full,
    // so currentIndex will not be -1 after loop
    currentIndex = -1;
    for (int i = 0; i < folders.size(); i++) {
      if (JVMCommonUtils.hasSpace(folders.get(i))) {
        currentIndex = i;
        break;
      }
    }
  }

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    int index = currentIndex;
    currentIndex = tryGetNextIndex(index);

    return index;
  }

  private int tryGetNextIndex(int start) throws DiskSpaceInsufficientException {
    int index = (start + 1) % folders.size();
    String dir = folders.get(index);
    while (!JVMCommonUtils.hasSpace(dir)) {
      File dirFile = FSFactoryProducer.getFSFactory().getFile(dir);

      Long lastPrintTime = dirLastPrintTimeMap.computeIfAbsent(index, i -> -1L);
      if (System.currentTimeMillis() - lastPrintTime > PRINT_INTERVAL_MS) {
        long freeSpace = dirFile.getFreeSpace();
        long totalSpace = dirFile.getTotalSpace();
        LOGGER.warn(
            "{} is above the warning threshold, free space {}, total space{}",
            dir,
            freeSpace,
            totalSpace);
        dirLastPrintTimeMap.put(index, System.currentTimeMillis());
      }
      if (index == start) {
        throw new DiskSpaceInsufficientException(folders);
      }
      index = (index + 1) % folders.size();
      dir = folders.get(index);
    }
    return index;
  }
}
