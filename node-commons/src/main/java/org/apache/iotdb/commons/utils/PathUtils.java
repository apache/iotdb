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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.PathParseException;
import org.apache.iotdb.tsfile.read.common.parser.PathNodesGenerator;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class PathUtils {

  /**
   * @param path the path will split. ex, root.ln.
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedNodes(String path) throws IllegalPathException {
    if ("".equals(path)) {
      return new String[] {};
    }
    try {
      return PathNodesGenerator.splitPathToNodes(path);
    } catch (PathParseException e) {
      throw new IllegalPathException(path);
    }
  }

  public static void isLegalPath(String path) throws IllegalPathException {
    try {
      PathNodesGenerator.splitPathToNodes(path);
    } catch (PathParseException e) {
      throw new IllegalPathException(path);
    }
  }

  /**
   * check whether measurement is legal according to syntax convention measurement can only be a
   * single node name
   */
  public static void isLegalSingleMeasurementLists(List<List<String>> measurementLists)
      throws MetadataException {
    if (measurementLists == null) {
      return;
    }
    for (List<String> measurements : measurementLists) {
      isLegalSingleMeasurements(measurements);
    }
  }

  /**
   * check whether measurement is legal according to syntax convention measurement can only be a
   * single node name
   */
  public static void isLegalSingleMeasurements(List<String> measurements) throws MetadataException {
    if (measurements == null) {
      return;
    }
    for (String measurement : measurements) {
      if (measurement == null) {
        continue;
      }
      if (measurement.startsWith(TsFileConstant.BACK_QUOTE_STRING)
          && measurement.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
        if (checkBackQuotes(measurement.substring(1, measurement.length() - 1))) {
          continue;
        } else {
          throw new IllegalPathException(measurement);
        }
      }
      if (IoTDBConstant.reservedWords.contains(measurement.toUpperCase())
          || StringUtils.isNumeric(measurement)
          || !TsFileConstant.NODE_NAME_PATTERN.matcher(measurement).matches()) {
        throw new IllegalPathException(measurement);
      }
    }
  }

  /**
   * check whether measurement is legal according to syntax convention measurement could be like a.b
   * (more than one node name), in template?
   */
  public static void isLegalMeasurementLists(List<List<String>> measurementLists)
      throws IllegalPathException {
    if (measurementLists == null) {
      return;
    }
    for (List<String> measurementList : measurementLists) {
      isLegalMeasurements(measurementList);
    }
  }

  /**
   * check whether measurement is legal according to syntax convention measurement could be like a.b
   * (more than one node name), in template?
   */
  public static void isLegalMeasurements(List<String> measurements) throws IllegalPathException {
    if (measurements == null) {
      return;
    }
    for (String measurement : measurements) {
      if (measurement != null) {
        PathUtils.isLegalPath(measurement);
      }
    }
  }

  public static boolean isStartWith(String deviceName, String storageGroup) {
    return deviceName.equals(storageGroup) || deviceName.startsWith(storageGroup + ".");
  }

  private static boolean checkBackQuotes(String src) {
    int num = src.length() - src.replace("`", "").length();
    if (num % 2 == 1) {
      return false;
    }
    return src.length() == (src.replace("``", "").length() + num);
  }
}
