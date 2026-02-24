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

package org.apache.iotdb.db.storageengine.load.disk;

import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.load.LoadFileException;

import java.io.File;

public interface ILoadDiskSelector {

  @FunctionalInterface
  interface DiskDirectorySelector {
    File selectDirectory(final File sourceDirectory, final String fileName, final int tierLevel)
        throws DiskSpaceInsufficientException, LoadFileException;
  }

  File selectTargetDirectory(
      final File sourceDirectory,
      final String fileName,
      final boolean appendFileName,
      final int tierLevel)
      throws DiskSpaceInsufficientException, LoadFileException;

  static ILoadDiskSelector initDiskSelector(
      final String selectStrategy, final String[] dirs, final DiskDirectorySelector selector) {
    final ILoadDiskSelector diskSelector;
    switch (ILoadDiskSelector.LoadDiskSelectorType.fromValue(selectStrategy)) {
      case INHERIT_SYSTEM_MULTI_DISKS_SELECT_STRATEGY:
        diskSelector = new InheritSystemMultiDisksStrategySelector(selector);
        break;
      case MIN_IO_FIRST:
      default:
        diskSelector = new MinIOSelector(dirs, selector);
    }
    return diskSelector;
  }

  enum LoadDiskSelectorType {
    MIN_IO_FIRST("MIN_IO_FIRST"),
    INHERIT_SYSTEM_MULTI_DISKS_SELECT_STRATEGY("INHERIT_SYSTEM_MULTI_DISKS_SELECT_STRATEGY"),
    // This type is specially designed for IoTV2 and Pipe, which means IoTV2 and Pipe will follow
    // the same strategy as ordinary load.
    INHERIT_LOAD("INHERIT_LOAD");

    private final String value;

    LoadDiskSelectorType(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static LoadDiskSelectorType fromValue(final String value) {
      if (value.equalsIgnoreCase(MIN_IO_FIRST.getValue())) {
        return MIN_IO_FIRST;
      } else if (value.equalsIgnoreCase(INHERIT_SYSTEM_MULTI_DISKS_SELECT_STRATEGY.getValue())) {
        return INHERIT_SYSTEM_MULTI_DISKS_SELECT_STRATEGY;
      } else if (value.equalsIgnoreCase(INHERIT_LOAD.getValue())) {
        return INHERIT_LOAD;
      }
      // return MIN_IO_FIRST by default
      return MIN_IO_FIRST;
    }
  }
}
