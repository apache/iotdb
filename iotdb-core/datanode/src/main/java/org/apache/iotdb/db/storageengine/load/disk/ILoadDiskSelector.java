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

import java.io.File;

public interface ILoadDiskSelector {

  File getTargetFile(
      File fileToLoad,
      String databaseName,
      String dataRegionId,
      long filePartitionId,
      String tsfileName)
      throws DiskSpaceInsufficientException;

  enum LoadDiskSelectorType {
    MIN_IO_FIRST("MIN_IO_FIRST"),
    DISK_STORAGE_BALANCE_FIRST("DISK_STORAGE_BALANCE_FIRST"),
    // This type is specially designed for IoTV2 and Pipe, which means IoTV2 and Pipe will follow
    // the same strategy as ordinary load.
    INHERIT_LOAD("INHERIT_LOAD");

    private final String value;

    LoadDiskSelectorType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static LoadDiskSelectorType fromValue(String value) {
      if (value.equalsIgnoreCase(MIN_IO_FIRST.getValue())) {
        return MIN_IO_FIRST;
      } else if (value.equalsIgnoreCase(DISK_STORAGE_BALANCE_FIRST.getValue())) {
        return DISK_STORAGE_BALANCE_FIRST;
      } else if (value.equalsIgnoreCase(INHERIT_LOAD.getValue())) {
        return INHERIT_LOAD;
      }
      // return DISK_STORAGE_BALANCE_FIRST by default
      return DISK_STORAGE_BALANCE_FIRST;
    }
  }
}
