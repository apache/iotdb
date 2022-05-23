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
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckpointFileUtils {
  public static final String FILE_PREFIX = "_";
  public static final String FILE_SUFFIX = IoTDBConstant.WAL_CHECKPOINT_FILE_SUFFIX;

  /**
   * versionId is a self-incremented id number, helping to maintain the order of checkpoint files
   */
  public static final Pattern CHECKPOINT_FILE_NAME_PATTERN =
      Pattern.compile("_(?<versionId>\\d+)\\.checkpoint");

  /** Return true when this file is .checkpoint file */
  public static boolean checkpointFilenameFilter(File dir, String name) {
    return CHECKPOINT_FILE_NAME_PATTERN.matcher(name).find();
  }

  /** List all .checkpoint files in the directory */
  public static File[] listAllCheckpointFiles(File dir) {
    return dir.listFiles(CheckpointFileUtils::checkpointFilenameFilter);
  }

  /** Parse version id from filename */
  public static int parseVersionId(String filename) {
    Matcher matcher = CHECKPOINT_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group("versionId"));
    }
    throw new RuntimeException("Invalid checkpoint file name: " + filename);
  }

  /** Sort checkpoint files by version id with descending order * */
  public static void descSortByVersionId(File[] checkpointFiles) {
    Arrays.sort(
        checkpointFiles,
        Comparator.comparingInt(file -> parseVersionId(((File) file).getName())).reversed());
  }

  /** Get .checkpoint filename */
  public static String getLogFileName(long version) {
    return FILE_PREFIX + version + FILE_SUFFIX;
  }
}
