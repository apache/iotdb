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
import org.apache.iotdb.tsfile.exception.PathParseException;
import org.apache.iotdb.tsfile.read.common.parser.PathNodesGenerator;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

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
      throw new PathParseException(ILLEGAL_PATH_ARGUMENT);
    }
    if (!needSplit) {
      // no split, we don't use antlr to check here.
      fullPath = pathSc;
    } else {
      if (pathSc.length() > 0) {
        String[] nodes = PathNodesGenerator.splitPathToNodes(pathSc);
        device = "";
        if (nodes.length > 1) {
          device = transformNodesToString(nodes, nodes.length - 1);
        }
        measurement = nodes[nodes.length - 1];
        fullPath = transformNodesToString(nodes, nodes.length);
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
   * @param needCheck need to validate the correctness of the path
   */
  public Path(String device, String measurement, boolean needCheck) {
    if (device == null || measurement == null) {
      throw new PathParseException(ILLEGAL_PATH_ARGUMENT);
    }
    if (!needCheck) {
      this.measurement = measurement;
      this.device = device;
      this.fullPath = device + "." + measurement;
      return;
    }
    // use PathNodesGenerator to check whether path is legal.
    if (!StringUtils.isEmpty(device) && !StringUtils.isEmpty(measurement)) {
      String path = device + TsFileConstant.PATH_SEPARATOR + measurement;
      String[] nodes = PathNodesGenerator.splitPathToNodes(path);
      this.device = transformNodesToString(nodes, nodes.length - 1);
      this.measurement = nodes[nodes.length - 1];
      this.fullPath = transformNodesToString(nodes, nodes.length);
    } else if (!StringUtils.isEmpty(device)) {
      String[] deviceNodes = PathNodesGenerator.splitPathToNodes(device);
      this.device = transformNodesToString(deviceNodes, deviceNodes.length);
      this.measurement = measurement;
      // for aligned path, sensor name for time column is ""
      this.fullPath = device + TsFileConstant.PATH_SEPARATOR + measurement;
    } else if (!StringUtils.isEmpty(measurement)) {
      String[] measurementNodes = PathNodesGenerator.splitPathToNodes(measurement);
      this.measurement = transformNodesToString(measurementNodes, measurementNodes.length);
      this.device = device;
      this.fullPath = measurement;
    } else {
      this.device = device;
      this.measurement = measurement;
      this.fullPath = "";
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

  /** return the column contained by this path */
  public int getColumnNum() {
    return 1;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write((byte) 3, byteBuffer); // org.apache.iotdb.db.metadata.path#PathType
    serializeWithoutType(byteBuffer);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write((byte) 3, stream); // org.apache.iotdb.db.metadata.path#PathType
    serializeWithoutType(stream);
  }

  public void serialize(PublicBAOS stream) throws IOException {
    ReadWriteIOUtils.write((byte) 3, stream); // org.apache.iotdb.db.metadata.path#PathType
    serializeWithoutType(stream);
  }

  protected void serializeWithoutType(ByteBuffer byteBuffer) {
    if (measurement == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(measurement, byteBuffer);
    }
    if (device == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(device, byteBuffer);
    }
    if (fullPath == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      ReadWriteIOUtils.write(fullPath, byteBuffer);
    }
  }

  protected void serializeWithoutType(OutputStream stream) throws IOException {
    if (measurement == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(measurement, stream);
    }
    if (device == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(device, stream);
    }
    if (fullPath == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(fullPath, stream);
    }
  }

  protected void serializeWithoutType(PublicBAOS stream) throws IOException {
    if (measurement == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(measurement, stream);
    }
    if (device == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(device, stream);
    }
    if (fullPath == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(fullPath, stream);
    }
  }

  public static Path deserialize(ByteBuffer byteBuffer) {
    Path path = new Path();
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    path.measurement = isNull == 0 ? null : ReadWriteIOUtils.readString(byteBuffer);
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    path.device = isNull == 0 ? null : ReadWriteIOUtils.readString(byteBuffer);
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    path.fullPath = isNull == 0 ? null : ReadWriteIOUtils.readString(byteBuffer);
    return path;
  }

  private String transformNodesToString(String[] nodes, int index) {
    Validate.isTrue(nodes.length > 0);
    StringBuilder s = new StringBuilder(nodes[0]);
    for (int i = 1; i < index; i++) {
      s.append(TsFileConstant.PATH_SEPARATOR);
      s.append(nodes[i]);
    }
    return s.toString();
  }
}
