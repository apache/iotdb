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

package org.apache.iotdb.itbase.constant;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;

import org.apache.tsfile.utils.FilePathUtils;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;

import java.io.File;

public class TestConstant {

  public static final String BASE_OUTPUT_PATH = "target".concat(File.separator);
  public static final String OUTPUT_DATA_DIR =
      BASE_OUTPUT_PATH.concat("data").concat(File.separator);
  public static final String PARTIAL_PATH_STRING =
      "%s" + File.separator + "%d" + File.separator + "%d" + File.separator;
  public static final String TEST_TSFILE_PATH =
      BASE_OUTPUT_PATH + "testTsFile".concat(File.separator) + PARTIAL_PATH_STRING;

  public static final String d0 = "root.vehicle.d0";
  public static final String s0 = "s0";
  public static final String s1 = "s1";
  public static final String s2 = "s2";
  public static final String s3 = "s3";
  public static final String s4 = "s4";
  public static final String s5 = "s5";
  public static final String d1 = "root.vehicle.d1";
  public static final String TIMESTAMP_STR = ColumnHeaderConstant.TIME;
  public static final String END_TIMESTAMP_STR = ColumnHeaderConstant.ENDTIME;
  public static final String DEVICE = ColumnHeaderConstant.DEVICE;
  public static boolean testFlag = true;
  public static String[] stringValue = new String[] {"A", "B", "C", "D", "E"};
  public static String[] booleanValue = new String[] {"true", "false"};
  public static final String TIMESERIES_STR = ColumnHeaderConstant.TIMESERIES;
  public static final String VALUE_STR = ColumnHeaderConstant.VALUE;
  public static final String DATA_TYPE_STR = ColumnHeaderConstant.DATATYPE;
  public static final String FUNCTION_TYPE_NATIVE = "native";
  public static final double DELTA = 1e-6;
  public static final double NULL = Double.MIN_VALUE;

  public static String[] createSql =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s5 WITH DATATYPE=DOUBLE, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      };

  public static String insertTemplate = "insert into %s(timestamp%s) values(%d%s)";

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

  public static String timeDuration(String path) {
    return String.format("time_duration(%s)", path);
  }

  public static String mode(String path) {
    return String.format("mode(%s)", path);
  }

  public static String stddev(String path) {
    return String.format("stddev(%s)", path);
  }

  public static String stddevPop(String path) {
    return String.format("stddev_pop(%s)", path);
  }

  public static String stddevSamp(String path) {
    return String.format("stddev_samp(%s)", path);
  }

  public static String variance(String path) {
    return String.format("variance(%s)", path);
  }

  public static String varPop(String path) {
    return String.format("var_pop(%s)", path);
  }

  public static String varSamp(String path) {
    return String.format("var_samp(%s)", path);
  }

  public static String countUDAF(String path) {
    return String.format("count_udaf(%s)", path);
  }

  public static String sumUDAF(String path) {
    return String.format("sum_udaf(%s)", path);
  }

  public static String avgUDAF(String path) {
    return String.format("avg_udaf(%s)", path);
  }

  public static String recordToInsert(TSRecord record) {
    StringBuilder measurements = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (DataPoint dataPoint : record.dataPointList) {
      measurements.append(",").append(dataPoint.getMeasurementId());
      values.append(",").append(dataPoint.getValue());
    }
    return String.format(insertTemplate, record.deviceId, measurements, record.time, values);
  }

  public static String getTestTsFilePath(
      String logicalStorageGroupName,
      long VirtualStorageGroupId,
      long TimePartitionId,
      long tsFileVersion) {
    String filePath =
        String.format(
            TEST_TSFILE_PATH, logicalStorageGroupName, VirtualStorageGroupId, TimePartitionId);
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + tsFileVersion
            + "-0-0.tsfile";
    return filePath.concat(fileName);
  }

  public static String getTestTsFileDir(
      String logicalStorageGroupName, long VirtualStorageGroupId, long TimePartitionId) {
    return String.format(
        TestConstant.TEST_TSFILE_PATH,
        logicalStorageGroupName,
        VirtualStorageGroupId,
        TimePartitionId);
  }
}
