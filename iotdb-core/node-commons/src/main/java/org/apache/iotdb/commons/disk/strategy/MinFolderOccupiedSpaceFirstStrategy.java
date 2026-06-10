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
package org.apache.iotdb.commons.disk.strategy;

import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.commons.utils.TestOnly;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MinFolderOccupiedSpaceFirstStrategy extends DirectoryStrategy {
  private static final long CANNOT_CALCULATE_OCCUPIED_SPACE_LOG_INTERVAL_MS = 3600 * 1000L;
  private static final ConcurrentMap<String, AtomicLong>
      CANNOT_CALCULATE_OCCUPIED_SPACE_LAST_LOG_TIME_MAP = new ConcurrentHashMap<>();

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    return getMinOccupiedSpaceFolder();
  }

  private int getMinOccupiedSpaceFolder() throws DiskSpaceInsufficientException {
    int minIndex = -1;
    long minSpace = Long.MAX_VALUE;

    for (int i = 0; i < folders.size(); i++) {
      String folder = folders.get(i);
      if (isUnavailableFolder(folder)) {
        continue;
      }
      if (!JVMCommonUtils.hasSpace(folder)) {
        continue;
      }

      long space = 0;
      try {
        space = JVMCommonUtils.getOccupiedSpace(folder);
        resetCannotCalculateOccupiedSpaceLogTime(folder);
      } catch (IOException e) {
        logCannotCalculateOccupiedSpaceIfNecessary(folder, e);
        continue;
      }
      if (space < minSpace) {
        minSpace = space;
        minIndex = i;
      }
    }

    if (minIndex == -1) {
      throw new DiskSpaceInsufficientException(folders);
    }

    return minIndex;
  }

  private static void logCannotCalculateOccupiedSpaceIfNecessary(String folder, IOException e) {
    AtomicLong lastLogTime =
        CANNOT_CALCULATE_OCCUPIED_SPACE_LAST_LOG_TIME_MAP.computeIfAbsent(
            folder, key -> new AtomicLong(0L));
    long now = System.currentTimeMillis();
    long previousLogTime = lastLogTime.get();
    if ((previousLogTime == 0
            || now - previousLogTime >= CANNOT_CALCULATE_OCCUPIED_SPACE_LOG_INTERVAL_MS)
        && lastLogTime.compareAndSet(previousLogTime, now)) {
      LOGGER.error(UtilMessages.CANNOT_CALCULATE_OCCUPIED_SPACE, folder, e);
    }
  }

  private static void resetCannotCalculateOccupiedSpaceLogTime(String folder) {
    CANNOT_CALCULATE_OCCUPIED_SPACE_LAST_LOG_TIME_MAP.remove(folder);
  }

  @TestOnly
  public static void resetCannotCalculateOccupiedSpaceLogTimes() {
    CANNOT_CALCULATE_OCCUPIED_SPACE_LAST_LOG_TIME_MAP.clear();
  }
}
