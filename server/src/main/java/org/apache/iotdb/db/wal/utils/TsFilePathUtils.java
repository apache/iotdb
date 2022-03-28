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
package org.apache.iotdb.db.wal.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import java.io.File;

public class TsFilePathUtils {
  private TsFilePathUtils() {}

  public static long getTimePartition(File tsFile) {
    File timePartitionFolder = tsFile.getParentFile();
    return Long.parseLong(timePartitionFolder.getName());
  }

  public static int getVirtualStorageGroupId(File tsFile) {
    File vsgFolder = tsFile.getParentFile().getParentFile();
    return Integer.parseInt(vsgFolder.getName());
  }

  public static String getStorageGroup(File tsFile) {
    File vsgFolder = tsFile.getParentFile().getParentFile().getParentFile();
    return vsgFolder.getName();
  }

  public static boolean isSequence(File tsFile) {
    File folder = tsFile.getParentFile().getParentFile().getParentFile().getParentFile();
    return folder.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME);
  }
}
