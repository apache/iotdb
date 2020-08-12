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
import java.util.ArrayList;
import java.util.List;
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
  private static final String illegalPathArgument = "Path parameter is null";
  private String alias = null;
  private String device = null;
  private String fullPath = null;
  private List<String> nodes;

  public Path() {}

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

  public Path(List<String> nodes) {
    this.nodes = nodes;
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
    this.fullPath = device + TsFileConstant.PATH_SEPARATOR + measurement;
    this.nodes = splitPathToDetachedPath(fullPath);
  }

  /**
   * extract device and measurement info from complete path string
   *
   * @param pathSc complete path string
   */
  private void init(String pathSc) {
    if(pathSc.equals("")) {
      this.nodes = new ArrayList<>();
      this.device = "";
      this.fullPath = "";
    } else {
      this.nodes = splitPathToDetachedPath(pathSc);
    }
  }

  public static List<String> splitPathToDetachedPath(String path) {
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        nodes.add(path.substring(startIndex, i));
        startIndex = i + 1;
      } else if (path.charAt(i) == '"') {
        int endIndex = path.indexOf('"', i + 1);
        if (endIndex != -1 && (endIndex == path.length() - 1 || path.charAt(endIndex + 1) == '.')) {
          nodes.add(path.substring(startIndex, endIndex + 1));
          i = endIndex + 1;
          startIndex = endIndex + 2;
        } else {
          throw new IllegalArgumentException("Illegal path: " + path);
        }
      } else if (path.charAt(i) == '\'') {
        throw new IllegalArgumentException("Illegal path with single quote: " + path);
      }
    }
    if (startIndex <= path.length() - 1) {
      nodes.add(path.substring(startIndex));
    }
    return nodes;
  }


  public static Path concatPath(Path src, Path tail) {
    if (tail.nodes.isEmpty()) {
      return src;
    }
    List<String> srcNodes = new ArrayList<>(src.nodes);
    srcNodes.addAll(tail.nodes);
    return new Path(srcNodes);
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  public String getFullPath() {
    if(fullPath != null) {
      return fullPath;
    } else {
      StringBuilder s = new StringBuilder(nodes.get(0));
      for(int i = 1; i < nodes.size(); i++) {
        s.append(TsFileConstant.PATH_SEPARATOR);
        s.append(nodes.get(i));
      }
      fullPath = s.toString();
      return fullPath;
    }
  }

  public List<String> getNodes() {
    return nodes;
  }

  public String getDevice() {
    if(device != null) {
      return device;
    } else if(nodes.size() <= 1) {
      this.device = "";
      return "";
    } else {
      StringBuilder s = new StringBuilder(nodes.get(0));
      for(int i = 1; i < nodes.size() - 1; i++) {
        s.append(TsFileConstant.PATH_SEPARATOR);
        s.append(nodes.get(i));
      }
      device = s.toString();
      return device;
    }
  }

  public String getMeasurement() {
    return nodes.get(nodes.size() - 1);
  }

  public String getAlias() { return alias; }

  public List<String> getDetachedPath() {
    return nodes;
  }

  public void setAlias(String alias) { this.alias = alias; }

  public String getFullPathWithAlias() {
    if(device != null) {
      return device + TsFileConstant.PATH_SEPARATOR + alias;
    } else {
      this.device = getDevice();
      return device + TsFileConstant.PATH_SEPARATOR + alias;
    }
  }

  @Override
  public int hashCode() {
    if(fullPath != null) {
      return fullPath.hashCode();
    } else {
      fullPath = getFullPath();
      return fullPath.hashCode();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof Path) {
      if(fullPath != null) {
        return fullPath.equals(((Path) obj).getFullPath());
      } else {
        for(int i = 0; i < nodes.size(); i++) {
          if(!nodes.get(i).equals(((Path) obj).nodes.get(i))) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  public boolean equals(String obj) {
    if(fullPath != null) {
      return fullPath.equals(obj);
    } else {
      return nodes.get(0).equals(obj);
    }
  }

  @Override
  public int compareTo(Path path) {
    if(fullPath == null) {
      this.fullPath = getFullPath();
    }
    return fullPath.compareTo(path.getFullPath());
  }

  @Override
  public String toString() {
    return getFullPath();
  }

  @Override
  public Path clone() {
    if (nodes != null) {
      return new Path(nodes);
    }
    return new Path(fullPath);
  }

}
