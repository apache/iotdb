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

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_FILE_PREFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_FILE_SUFFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_START_SEARCH_INDEX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_VERSION_ID;

public class WALFileUtils {
  /**
   * versionId is a self-incremented id number, helping to maintain the order of wal files.
   * startSearchIndex is the valid search index of last flushed wal entry. For example: <br>
   * _0-0.wal: 1, 2, 3, -1, -1, 4, 5, -1 <br>
   * _1-5.wal: -1, -1, -1, -1 <br>
   * _2-5.wal: 6, 7, 8, 9, -1, -1, -1, 10, 11, -1, 12, 12 <br>
   * _3-12.wal: 12, 12, 12, 12, 12 <br>
   * _4-12.wal: 12, 13, 14, 15, 16, -1 <br>
   */
  public static final Pattern WAL_FILE_NAME_PATTERN =
      Pattern.compile(
          String.format(
              "%s(?<%s>\\d+)-(?<%s>\\d+)\\%s$",
              WAL_FILE_PREFIX, WAL_VERSION_ID, WAL_START_SEARCH_INDEX, WAL_FILE_SUFFIX));

  /** Return true when this file is .wal file */
  public static boolean walFilenameFilter(File dir, String name) {
    return WAL_FILE_NAME_PATTERN.matcher(name).find();
  }

  /** List all .wal files in the directory */
  public static File[] listAllWALFiles(File dir) {
    return dir.listFiles(WALFileUtils::walFilenameFilter);
  }

  /** Parse version id from filename */
  public static int parseVersionId(String filename) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(WAL_VERSION_ID));
    }
    throw new RuntimeException("Invalid wal file name: " + filename);
  }

  /** Parse start search index from filename */
  public static long parseStartSearchIndex(String filename) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(WAL_START_SEARCH_INDEX));
    }
    throw new RuntimeException("Invalid wal file name: " + filename);
  }

  /** Sort wal files by version id with ascending order */
  public static void ascSortByVersionId(File[] walFiles) {
    Arrays.sort(walFiles, Comparator.comparingInt(file -> parseVersionId(file.getName())));
  }

  /**
   * Find index of the file which probably contains target insert plan. <br>
   * Given wal files [ _0-0.wal, _1-5.wal, _2-5.wal, _3-12.wal, _4-12.wal ], details as below: <br>
   * _0-0.wal: 1, 2, 3, -1, -1, 4, 5, -1 <br>
   * _1-5.wal: -1, -1, -1, -1 <br>
   * _2-5.wal: 6, 7, 8, 9, -1, -1, -1, 10, 11, -1, 12, 12 <br>
   * _3-12.wal: 12, 12, 12, 12, 12 <br>
   * _4-12.wal: 12, 13, 14, 15, 16, -1 <br>
   * searching [1, 5] will return 0, searching [6, 12] will return 1, search [13, infinity) will
   * return 3， others will return -1
   *
   * @param files files to be searched
   * @param targetSearchIndex search index of target insert plan
   * @return index of the file which probably contains target insert plan , -1 if the target insert
   *     plan definitely doesn't exist
   */
  public static int binarySearchFileBySearchIndex(File[] files, long targetSearchIndex) {
    if (files == null
        || files.length == 0
        || targetSearchIndex <= parseStartSearchIndex(files[0].getName())) {
      return -1;
    }

    if (targetSearchIndex > parseStartSearchIndex(files[files.length - 1].getName())) {
      return files.length - 1;
    }

    int low = 0;
    int high = files.length - 1;
    // search file whose search index i < targetSearchIndex
    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = parseStartSearchIndex(files[mid].getName());

      if (midVal < targetSearchIndex) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return low - 1;
  }

  /** Get .wal filename */
  public static String getLogFileName(int versionId, long startSearchIndex) {
    return WAL_FILE_PREFIX + versionId + FILE_NAME_SEPARATOR + startSearchIndex + WAL_FILE_SUFFIX;
  }
}
