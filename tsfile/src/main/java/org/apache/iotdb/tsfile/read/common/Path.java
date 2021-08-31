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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.Serializable;

/**
 * This class represent a time series in TsFile, which is usually defined by a device and a
 * measurement.
 *
 * <p>If you want to use one String such as "device1.measurement1" to init Path in TsFile API,
 * please use the new Path(string, true) to split it to device and measurement.
 */
public class Path implements Serializable, Comparable<Path> {

  private static final long serialVersionUID = 3405277066329298200L;
  private String measurement;
  protected String device;
  protected String fullPath;
  private static final String ILLEGAL_PATH_ARGUMENT = "Path parameter is null";

  public Path() {}

  /**
   * this constructor doesn't split the path, only useful for table header.
   *
   * @param pathSc the path that wouldn't be split.
   */
  @SuppressWarnings("the path that wouldn't be split")
  public Path(String pathSc) {
    this(pathSc, false);
  }

  /**
   * @param pathSc path
   * @param needSplit whether need to be split to device and measurement, doesn't support escape
   *     character yet.
   */
  public Path(String pathSc, boolean needSplit) {
    if (pathSc == null) {
      throw new IllegalArgumentException(ILLEGAL_PATH_ARGUMENT);
    }
    if (!needSplit) {
      fullPath = pathSc;
    } else {
      if (pathSc.length() > 0) {
        if (pathSc.charAt(pathSc.length() - 1) == TsFileConstant.DOUBLE_QUOTE) {
          int endIndex = pathSc.lastIndexOf('"', pathSc.length() - 2);
          // if a double quotes with escape character
          while (endIndex != -1 && pathSc.charAt(endIndex - 1) == '\\') {
            endIndex = pathSc.lastIndexOf('"', endIndex - 2);
          }
          if (endIndex != -1 && (endIndex == 0 || pathSc.charAt(endIndex - 1) == '.')) {
            fullPath = pathSc;
            device = pathSc.substring(0, endIndex - 1);
            measurement = pathSc.substring(endIndex);
          } else {
            throw new IllegalArgumentException(ILLEGAL_PATH_ARGUMENT);
          }
        } else if (pathSc.charAt(pathSc.length() - 1) != TsFileConstant.DOUBLE_QUOTE
            && pathSc.charAt(pathSc.length() - 1) != TsFileConstant.PATH_SEPARATOR_CHAR) {
          int endIndex = pathSc.lastIndexOf(TsFileConstant.PATH_SEPARATOR_CHAR);
          if (endIndex < 0) {
            fullPath = pathSc;
            device = "";
            measurement = pathSc;
          } else {
            fullPath = pathSc;
            device = pathSc.substring(0, endIndex);
            measurement = pathSc.substring(endIndex + 1);
          }
        } else {
          throw new IllegalArgumentException(ILLEGAL_PATH_ARGUMENT);
        }
      } else {
        fullPath = pathSc;
        device = "";
        measurement = pathSc;
      }
    }
  }

  /**
   * construct a Path directly using device and measurement, no need to reformat the path
   *
   * @param device root.deviceType.d1
   * @param measurement s1 , does not contain TsFileConstant.PATH_SEPARATOR
   */
  public Path(String device, String measurement) {
    if (device == null || measurement == null) {
      throw new IllegalArgumentException(ILLEGAL_PATH_ARGUMENT);
    }
    this.device = device;
    this.measurement = measurement;
    if (!device.equals("")) {
      this.fullPath = device + TsFileConstant.PATH_SEPARATOR + measurement;
    } else {
      fullPath = measurement;
    }
  }

  public String getFullPath() {
    return fullPath;
  }

  public String getDevice() {
    return device;
  }

  public String getMeasurement() {
    return measurement;
  }

  public String getFullPathWithAlias() {
    throw new IllegalArgumentException("doesn't alias in TSFile Path");
  }

  public void setMeasurement(String measurement) {
    this.measurement = measurement;
  }

  @Override
  public int hashCode() {
    return fullPath.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Path && this.fullPath.equals(((Path) obj).fullPath);
  }

  public boolean equals(String obj) {
    return this.fullPath.equals(obj);
  }

  @Override
  public int compareTo(Path path) {
    return fullPath.compareTo(path.getFullPath());
  }

  @Override
  public String toString() {
    return fullPath;
  }

  @Override
  public Path clone() {
    return new Path(fullPath);
  }
}
