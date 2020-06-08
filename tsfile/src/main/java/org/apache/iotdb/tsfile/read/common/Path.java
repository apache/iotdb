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

import java.io.Serializable;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * This class define an Object named Path to represent a series in IoTDB.
 * AndExpression in batch read, this definition is also used in query
 * processing. Note that, Path is unmodified after a new object has been
 * created.
 */
public class Path implements Serializable, Comparable<Path> {

  private static final long serialVersionUID = 3405277066329298200L;
  private String measurement = null;
  private String alias = null;
  private String device = null;
  private String fullPath;
  private static final String illegalPathArgument = "Path parameter is null";

  public Path(StringContainer pathSc) {
    if (pathSc == null) {
      throw new IllegalArgumentException("input pathSc is null!");
    }

    init(pathSc.toString());
  }

  public Path(String pathSc) {
    if (pathSc == null) {
      throw new IllegalArgumentException(illegalPathArgument);
    }
    init(pathSc);
  }

  public Path(String[] pathSc) {
    if (pathSc == null) {
      throw new IllegalArgumentException(illegalPathArgument);
    }
    init(new StringContainer(pathSc, TsFileConstant.PATH_SEPARATOR).toString());
  }

  /**
   * construct a Path directly using device and measurement, no need to reformat
   * the path
   *
   * @param device      root.deviceType.d1
   * @param measurement s1 , does not contain TsFileConstant.PATH_SEPARATOR
   */
  public Path(String device, String measurement) {
    if (device == null || measurement == null) {
      throw new IllegalArgumentException(illegalPathArgument);
    }
    this.device = device;
    this.measurement = measurement;
    this.fullPath = device + TsFileConstant.PATH_SEPARATOR
        + (measurement.contains(TsFileConstant.PATH_SEPARATOR) ? "\"" + measurement + "\"" : measurement);
  }

  /**
   * extract device and measurement info from complete path string
   *
   * @param pathSc complete path string
   */
  private void init(String pathSc) {
    int i = 0;
    int j = 0;
    for (char c : pathSc.toCharArray()) {
      if (c == '\"') {
        i++;
      } else if (c == '\'') {
        j++;
      }
    }
    if ((i != 2 && i != 0) || (j != 2 && j != 0)) {
      throw new IllegalArgumentException("input pathSc single/double quotes error, not in pair or more than one pair!");
    }
    if ((i == 2 && pathSc.length() - 1 != pathSc.lastIndexOf("\""))
        || (j == 2 && pathSc.length() - 1 != pathSc.lastIndexOf("\'"))) {
      throw new IllegalArgumentException("input pathSc contains quoted string in the middle!");
    }
    String[] subStrs;
    if (i != 0 || j != 0) {
      if (i == 2) {
        subStrs = pathSc.split("\"");
      } else {
        subStrs = pathSc.split("\'");
      }
      device = subStrs[0];
      if (!device.equals("")) {
        device = device.substring(0, subStrs[0].length() - 1);
      }
      measurement = subStrs[1];
      fullPath = pathSc;
    } else {
      StringContainer sc = new StringContainer(pathSc.split(TsFileConstant.PATH_SEPARATER_NO_REGEX),
          TsFileConstant.PATH_SEPARATOR);
      if (sc.size() <= 1) {
        device = "";
        fullPath = measurement = sc.toString();
      } else {
        device = sc.getSubStringContainer(0, -2).toString();
        measurement = sc.getSubString(-1);
        fullPath = sc.toString();
      }
    }
  }

  public static Path mergePath(Path prefix, Path suffix) {
    if (suffix.fullPath.equals("")) {
      return prefix;
    } else if (prefix.fullPath.equals("")) {
      return suffix;
    }
    StringContainer sc = new StringContainer(TsFileConstant.PATH_SEPARATOR);
    sc.addTail(prefix);
    sc.addTail(suffix);
    return new Path(sc);
  }

  /**
   * add {@code prefix} as the prefix of {@code src}.
   *
   * @param src    to be added.
   * @param prefix the newly prefix
   * @return if this path start with prefix
   */
  public static Path addPrefixPath(Path src, String prefix) {
    if (prefix.equals("")) {
      return src;
    }
    StringContainer sc = new StringContainer(TsFileConstant.PATH_SEPARATOR);
    sc.addTail(prefix);
    sc.addTail(src);
    return new Path(sc);
  }

  /**
   * add {@code prefix} as the prefix of {@code src}.
   *
   * @param src    to be added.
   * @param prefix the newly prefix
   * @return <code>Path</code>
   */
  public static Path addPrefixPath(Path src, Path prefix) {
    return addPrefixPath(src, prefix.fullPath);
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

  public String getAlias() { return alias; }

  public void setAlias(String alias) { this.alias = alias; }

  public String getFullPathWithAlias() { return device + TsFileConstant.PATH_SEPARATOR + alias; }

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

  /**
   * if prefix is null, return false, else judge whether this.fullPath starts with
   * prefix
   *
   * @param prefix the prefix string to be tested.
   * @return True if fullPath starts with prefix
   */
  public boolean startWith(String prefix) {
    return prefix != null && fullPath.startsWith(prefix);
  }

  /**
   * if prefix is null, return false, else judge whether this.fullPath starts with
   * prefix.fullPath
   *
   * @param prefix the prefix path to be tested.
   * @return True if fullPath starts with prefix.fullPath
   */
  public boolean startWith(Path prefix) {
    return startWith(prefix.fullPath);
  }

}
