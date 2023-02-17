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
package org.apache.iotdb.db.metadata.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.exception.metadata.IllegalParameterOfPathException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.LOSS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.SDT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.SDT_COMP_DEV;
import static org.apache.iotdb.commons.conf.IoTDBConstant.SDT_COMP_MAX_TIME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.SDT_COMP_MIN_TIME;

public class MetaFormatUtils {

  private static final Logger logger = LoggerFactory.getLogger(MetaFormatUtils.class);

  public static String[] RESERVED_NODE_NAMES = {"time", "timestamp"};

  /** check whether the given path is well formatted */
  public static void checkTimeseries(PartialPath timeseries) throws IllegalPathException {
    try {
      checkCharacters(timeseries.getFullPath());
    } catch (MetadataException e) {
      throw new IllegalPathException(timeseries.getFullPath(), e.getMessage());
    }
    for (String name : timeseries.getNodes()) {
      try {
        checkNameFormat(name);
        checkReservedNames(name);
      } catch (MetadataException e) {
        throw new IllegalPathException(timeseries.getFullPath(), e.getMessage());
      }
    }
  }

  /** check whether the node name uses "." or "*" correctly */
  private static void checkNameFormat(String name) throws MetadataException {
    if (!((name.startsWith("`") && name.endsWith("`")))
        && (name.contains(".") || name.contains("*"))) {
      throw new MetadataException(String.format("%s is an illegal name.", name));
    }
  }

  /** check the characters in path or single node */
  private static void checkCharacters(String timeseries) throws MetadataException {
    if (!IoTDBConfig.NODE_PATTERN.matcher(timeseries).matches()) {
      throw new MetadataException(
          String.format("The name, %s, contains unsupported character.", timeseries));
    }
  }

  /** check whether the node name uses the reserved name */
  private static void checkReservedNames(String name) throws MetadataException {
    String processedName = name.trim().toLowerCase(Locale.ENGLISH);
    for (String reservedName : RESERVED_NODE_NAMES) {
      if (reservedName.equals(processedName)) {
        throw new MetadataException(String.format("%s is an illegal name.", name));
      }
    }
  }

  /** check whether the node name is well formatted */
  public static void checkNodeName(String name) throws MetadataException {
    checkCharacters(name);
    checkReservedNames(name);
    checkNameFormat(name);
  }

  /** check whether the measurement ids in schema is well formatted */
  public static void checkSchemaMeasurementNames(List<String> measurements)
      throws MetadataException {
    for (String measurement : measurements) {
      checkNodeName(measurement);
    }
  }

  /** check whether the storageGroup name uses illegal characters */
  public static void checkStorageGroup(String storageGroup) throws IllegalPathException {
    if (!IoTDBConfig.STORAGE_GROUP_PATTERN.matcher(storageGroup).matches()) {
      throw new IllegalPathException(
          String.format(
              "The database name can only be characters, numbers and underscores. %s",
              storageGroup));
    }
  }

  /** check props when creating timeseries */
  public static void checkTimeseriesProps(String path, Map<String, String> props)
      throws IllegalParameterOfPathException {
    if (props != null && props.containsKey(LOSS) && props.get(LOSS).equals(SDT)) {
      checkSDTFormat(path, props);
    }
  }

  // check if sdt parameters are valid
  private static void checkSDTFormat(String path, Map<String, String> props)
      throws IllegalParameterOfPathException {
    if (!props.containsKey(SDT_COMP_DEV)) {
      throw new IllegalParameterOfPathException("SDT compression deviation is required", path);
    }

    try {
      double d = Double.parseDouble(props.get(SDT_COMP_DEV));
      if (d < 0) {
        throw new IllegalParameterOfPathException(
            "SDT compression deviation cannot be negative", path);
      }
    } catch (NumberFormatException e) {
      throw new IllegalParameterOfPathException("SDT compression deviation formatting error", path);
    }

    long compMinTime = sdtCompressionTimeFormat(SDT_COMP_MIN_TIME, props, path);
    long compMaxTime = sdtCompressionTimeFormat(SDT_COMP_MAX_TIME, props, path);

    if (compMaxTime <= compMinTime) {
      throw new IllegalParameterOfPathException(
          "SDT compression maximum time needs to be greater than compression minimum time", path);
    }
  }

  private static long sdtCompressionTimeFormat(
      String compTime, Map<String, String> props, String path)
      throws IllegalParameterOfPathException {
    boolean isCompMaxTime = compTime.equals(SDT_COMP_MAX_TIME);
    long time = isCompMaxTime ? Long.MAX_VALUE : 0;
    String s = isCompMaxTime ? "maximum" : "minimum";
    if (props.containsKey(compTime)) {
      try {
        time = Long.parseLong(props.get(compTime));
        if (time < 0) {
          throw new IllegalParameterOfPathException(
              String.format("SDT compression %s time cannot be negative", s), path);
        }
      } catch (IllegalParameterOfPathException e) {
        throw new IllegalParameterOfPathException(
            String.format("SDT compression %s time formatting error", s), path);
      }
    } else {
      logger.info("{} enabled SDT but did not set compression {} time", path, s);
    }
    return time;
  }
}
