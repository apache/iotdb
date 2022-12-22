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
import org.apache.iotdb.tsfile.read.common.parser.PathVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  public static String[] isLegalPath(String path) throws IllegalPathException {
    try {
      return PathNodesGenerator.splitPathToNodes(path);
    } catch (PathParseException e) {
      throw new IllegalPathException(path);
    }
  }

  /**
   * check whether measurement is legal according to syntax convention. Measurement can only be a
   * single node name. The returned list is updated, could be different from the original list.
   */
  public static List<List<String>> checkIsLegalSingleMeasurementListsAndUpdate(
      List<List<String>> measurementLists) throws MetadataException {
    if (measurementLists == null) {
      return null;
    }
    // skip checking duplicated measurements
    Map<String, String> checkedMeasurements = new HashMap<>();
    List<List<String>> res = new ArrayList<>();
    for (List<String> measurements : measurementLists) {
      res.add(checkLegalSingleMeasurementsAndSkipDuplicate(measurements, checkedMeasurements));
    }
    return res;
  }

  /**
   * check whether measurement is legal according to syntax convention. Measurement can only be a
   * single node name, use set to skip checking duplicated measurements
   */
  public static List<String> checkLegalSingleMeasurementsAndSkipDuplicate(
      List<String> measurements, Map<String, String> checkedMeasurements) throws MetadataException {
    if (measurements == null) {
      return null;
    }
    List<String> res = new ArrayList<>();
    for (String measurement : measurements) {
      if (measurement == null) {
        res.add(null);
        continue;
      }
      if (checkedMeasurements.get(measurement) != null) {
        res.add(checkedMeasurements.get(measurement));
        continue;
      }
      String checked = checkAndReturnSingleMeasurement(measurement);
      checkedMeasurements.put(measurement, checked);
      res.add(checked);
    }
    return res;
  }

  /**
   * check whether measurement is legal according to syntax convention. Measurement can only be a
   * single node name.
   */
  public static List<String> checkIsLegalSingleMeasurementsAndUpdate(List<String> measurements)
      throws MetadataException {
    if (measurements == null) {
      return null;
    }
    List<String> res = new ArrayList<>();
    for (String measurement : measurements) {
      if (measurement == null) {
        continue;
      }
      res.add(checkAndReturnSingleMeasurement(measurement));
    }
    return res;
  }

  /**
   * check whether measurement is legal according to syntax convention measurement could be like a.b
   * (more than one node name), in template?
   */
  public static List<List<String>> checkIsLegalMeasurementListsAndUpdate(
      List<List<String>> measurementLists) throws IllegalPathException {
    if (measurementLists == null) {
      return null;
    }
    List<List<String>> res = new ArrayList<>();
    for (List<String> measurementList : measurementLists) {
      res.add(checkIsLegalMeasurementsAndUpdate(measurementList));
    }
    return res;
  }

  /**
   * check whether measurement is legal according to syntax convention measurement could be like a.b
   * (more than one node name), in template?
   */
  public static List<String> checkIsLegalMeasurementsAndUpdate(List<String> measurements)
      throws IllegalPathException {
    if (measurements == null) {
      return null;
    }
    List<String> res = new ArrayList<>();
    for (String measurement : measurements) {
      if (measurement != null) {
        res.add(PathUtils.isLegalPath(measurement)[0]);
      }
    }
    return res;
  }

  /** check a measurement and update it if needed to. for example: `sd` -> sd */
  public static String checkAndReturnSingleMeasurement(String measurement)
      throws IllegalPathException {
    if (measurement == null) {
      return null;
    }
    if (measurement.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && measurement.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      if (checkBackQuotes(measurement.substring(1, measurement.length() - 1))) {
        return removeBackQuotesIfNecessary(measurement);
      } else {
        throw new IllegalPathException(measurement);
      }
    }
    if (IoTDBConstant.reservedWords.contains(measurement.toUpperCase())
        || isRealNumber(measurement)
        || !TsFileConstant.NODE_NAME_PATTERN.matcher(measurement).matches()) {
      throw new IllegalPathException(measurement);
    }
    return measurement;
  }

  /** Return true if the str is a real number. Examples: 1.0; +1.0; -1.0; 0011; 011e3; +23e-3 */
  public static boolean isRealNumber(String str) {
    return PathVisitor.isRealNumber(str);
  }

  public static boolean isStartWith(String deviceName, String storageGroup) {
    return deviceName.equals(storageGroup) || deviceName.startsWith(storageGroup + ".");
  }

  /** Remove the back quotes of a measurement if necessary */
  public static String removeBackQuotesIfNecessary(String measurement) {
    String unWrapped = measurement.substring(1, measurement.length() - 1);
    if (PathUtils.isRealNumber(unWrapped)
        || !TsFileConstant.IDENTIFIER_PATTERN.matcher(unWrapped).matches()) {
      return measurement;
    } else {
      return unWrapped;
    }
  }

  private static boolean checkBackQuotes(String src) {
    int num = src.length() - src.replace("`", "").length();
    if (num % 2 == 1) {
      return false;
    }
    return src.length() == (src.replace("``", "").length() + num);
  }
}
