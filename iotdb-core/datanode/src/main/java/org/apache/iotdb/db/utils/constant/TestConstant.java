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

package org.apache.iotdb.db.utils.constant;

import org.apache.tsfile.utils.FilePathUtils;

import java.io.File;

public class TestConstant {

  public static final String BASE_OUTPUT_PATH = baseOutputDirectory();
  public static final String OUTPUT_DATA_DIR =
      BASE_OUTPUT_PATH.concat("data").concat(File.separator);
  public static final String PARTIAL_PATH_STRING =
      "%s" + File.separator + "%d" + File.separator + "%d" + File.separator;
  public static final String TEST_TSFILE_PATH =
      BASE_OUTPUT_PATH + "testTsFile".concat(File.separator) + PARTIAL_PATH_STRING;

  public static final String TIMESTAMP_STR = "Time";

  public static String firstValue(String path) {
    return String.format("first_value(%s)", path);
  }

  public static String lastValue(String path) {
    return String.format("last_value(%s)", path);
  }

  public static String sum(String path) {
    return String.format("sum(%s)", path);
  }

  public static String avg(String path) {
    return String.format("avg(%s)", path);
  }

  public static String count(String path) {
    return String.format("count(%s)", path);
  }

  public static String maxTime(String path) {
    return String.format("max_time(%s)", path);
  }

  public static String minTime(String path) {
    return String.format("min_time(%s)", path);
  }

  public static String maxValue(String path) {
    return String.format("max_value(%s)", path);
  }

  public static String extreme(String path) {
    return String.format("extreme(%s)", path);
  }

  public static String minValue(String path) {
    return String.format("min_value(%s)", path);
  }

  public static String maxBy(String x, String y) {
    return String.format("max_by(%s, %s)", x, y);
  }

  public static String minBy(String x, String y) {
    return String.format("min_by(%s, %s)", x, y);
  }

  private TestConstant() {}

  public static String getTestTsFilePath(
      String logicalStorageGroupName,
      long virtualStorageGroupId,
      long timePartitionId,
      long tsFileVersion) {
    String filePath =
        String.format(
            TEST_TSFILE_PATH, logicalStorageGroupName, virtualStorageGroupId, timePartitionId);
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + tsFileVersion
            + "-0-0.tsfile";
    return filePath.concat(fileName);
  }

  public static String getTestTsFileDir(
      String logicalStorageGroupName, long virtualStorageGroupId, long timePartitionId) {
    return String.format(
        TestConstant.TEST_TSFILE_PATH,
        logicalStorageGroupName,
        virtualStorageGroupId,
        timePartitionId);
  }

  private static String baseOutputDirectory() {
    File dir = new File("target");
    dir.mkdirs();
    return dir.getPath().concat(File.separator);
  }
}
