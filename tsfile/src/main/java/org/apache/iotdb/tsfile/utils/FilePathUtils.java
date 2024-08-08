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

package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import java.io.File;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class FilePathUtils {

  private static final String PATH_SPLIT_STRING =
      TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs() == FSType.LOCAL
              && "\\".equals(File.separator)
          ? "\\\\"
          : "/";
  public static final String FILE_NAME_SEPARATOR = "-";

  private FilePathUtils() {
    // forbidding instantiation
  }

  /**
   * Format file path to end with File.separator
   *
   * @param filePath origin file path
   * @return Regularized Path
   */
  public static String regularizePath(String filePath) {
    if (filePath.length() > 0 && filePath.charAt(filePath.length() - 1) != File.separatorChar) {
      filePath = filePath + File.separatorChar;
    }
    return filePath;
  }

  /**
   * IMPORTANT, when the path of TsFile changes, the following methods should be changed
   * accordingly. The sequence TsFile is located at ${IOTDB_DATA_DIR}/data/sequence/. The unsequence
   * TsFile is located at ${IOTDB_DATA_DIR}/data/unsequence/. Where different database's TsFile is
   * located at <logicalStorageGroupName>/<virtualStorageGroupName>/<timePartitionId>/<fileName>.
   * For example, one sequence TsFile may locate at
   * /data/data/sequence/root.group_9/0/0/1611199237113-4-0.tsfile
   *
   * @param tsFileAbsolutePath the tsFile Absolute Path
   */
  public static String[] splitTsFilePath(String tsFileAbsolutePath) {
    return tsFileAbsolutePath.split(PATH_SPLIT_STRING);
  }

  public static String getLogicalStorageGroupName(String tsFileAbsolutePath) {
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    return pathSegments[pathSegments.length - 4];
  }

  public static String getDataRegionId(String tsFileAbsolutePath) {
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    return pathSegments[pathSegments.length - 3];
  }

  public static long getTimePartitionId(String tsFileAbsolutePath) {
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    return Long.parseLong(pathSegments[pathSegments.length - 2]);
  }

  /**
   * @param tsFileAbsolutePath the Remote TsFile Absolute Path
   * @return the file in the snapshot is a hardlink, remove the hardlink suffix
   */
  public static String getTsFileNameWithoutHardLink(String tsFileAbsolutePath) {
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    return pathSegments[pathSegments.length - 1].substring(
        0, pathSegments[pathSegments.length - 1].lastIndexOf(TsFileConstant.PATH_SEPARATOR));
  }

  public static String getTsFilePrefixPath(String tsFileAbsolutePath) {
    if (tsFileAbsolutePath == null) {
      return null;
    }
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    int pathLength = pathSegments.length;
    return pathSegments[pathLength - 4]
        + File.separator
        + pathSegments[pathLength - 3]
        + File.separator
        + pathSegments[pathLength - 2];
  }

  public static long splitAndGetTsFileVersion(String tsFileName) {
    String[] names = tsFileName.split(FILE_NAME_SEPARATOR);
    if (names.length != 4) {
      return 0;
    }
    return Long.parseLong(names[1]);
  }

  /**
   * @return a long array whose length is 2, the first long value is tsfile version, second long
   *     value is compaction version, high 32 bit is in-space compaction count, low 32 bit is
   *     cross-space compaction count
   */
  private static long[] splitAndGetVersionArray(String tsFileName) {
    String[] names = tsFileName.split(FILE_NAME_SEPARATOR);
    long[] versionArray = new long[2];
    if (names.length != 4) {
      return versionArray;
    }
    versionArray[0] = Long.parseLong(names[1]);
    versionArray[1] =
        (Long.parseLong(names[2]) << 32)
            | Long.parseLong(names[3].substring(0, names[3].length() - TSFILE_SUFFIX.length()));
    return versionArray;
  }

  public static Pair<String, Long> getLogicalSgNameAndTimePartitionIdPair(
      String tsFileAbsolutePath) {
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    return new Pair<>(
        pathSegments[pathSegments.length - 4],
        Long.parseLong(pathSegments[pathSegments.length - 2]));
  }

  /**
   * pair.left tsFilePrefixPath, like data/data/sequence/root.sg1/0/0 pair.right is a long array
   * whose length is 2 pair.right[0] is tsfile version pair.right[1] is compaction version, high 32
   * bit is compaction level, low 32 bit is merge count
   */
  public static Pair<String, long[]> getTsFilePrefixPathAndTsFileVersionPair(
      String tsFileAbsolutePath) {
    String[] pathSegments = splitTsFilePath(tsFileAbsolutePath);
    int pathLength = pathSegments.length;
    return new Pair<>(
        pathSegments[pathLength - 4]
            + File.separator
            + pathSegments[pathLength - 3]
            + File.separator
            + pathSegments[pathLength - 2],
        splitAndGetVersionArray(pathSegments[pathLength - 1]));
  }
}
