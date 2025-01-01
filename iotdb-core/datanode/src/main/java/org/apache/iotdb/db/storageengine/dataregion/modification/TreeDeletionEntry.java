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
package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

public class TreeDeletionEntry extends ModEntry {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeDeletionEntry.class);
  private MeasurementPath pathPattern;

  public TreeDeletionEntry() {
    super(ModType.TREE_DELETION);
  }

  public TreeDeletionEntry(MeasurementPath path, long start, long end) {
    this(path, new TimeRange(start, end));
  }

  public TreeDeletionEntry(MeasurementPath path, TimeRange timeRange) {
    this();
    this.pathPattern = path;
    this.timeRange = timeRange;
  }

  @TestOnly
  public TreeDeletionEntry(MeasurementPath path, long endTime) {
    this();
    this.pathPattern = path;
    this.timeRange = new TimeRange(Long.MIN_VALUE, endTime);
  }

  public TreeDeletionEntry(TreeDeletionEntry another) {
    this(
        another.pathPattern, new TimeRange(another.timeRange.getMin(), another.timeRange.getMax()));
  }

  public TreeDeletionEntry(Deletion deletion) {
    this(deletion.getPath(), deletion.getTimeRange());
  }

  @Override
  public int serializedSize() {
    String patternFullPath = pathPattern.getFullPath();
    int length = patternFullPath.length();
    return super.serializedSize()
        + ReadWriteForEncodingUtils.varIntSize(length)
        + length * Character.BYTES;
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    long size = super.serialize(stream);
    size += ReadWriteIOUtils.writeVar(pathPattern.getFullPath(), stream);
    return size;
  }

  @Override
  public long serialize(ByteBuffer buffer) {
    long size = super.serialize(buffer);
    size += ReadWriteIOUtils.writeVar(pathPattern.getFullPath(), buffer);
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    super.deserialize(stream);
    try {
      this.pathPattern = new MeasurementPath(ReadWriteIOUtils.readVarIntString(stream));
    } catch (IllegalPathException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    super.deserialize(buffer);
    try {
      this.pathPattern = new MeasurementPath(ReadWriteIOUtils.readVarIntString(buffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public boolean matches(PartialPath path) {
    return pathPattern.matchFullPath(path);
  }

  @Override
  public boolean affects(IDeviceID deviceID, long startTime, long endTime) {
    return affects(deviceID)
        && ModificationUtils.overlap(getStartTime(), getEndTime(), startTime, endTime);
  }

  @Override
  public boolean affects(IDeviceID deviceID) {
    try {
      if (pathPattern.endWithMultiLevelWildcard()) {
        PartialPath deviceIdPath = new PartialPath(deviceID);
        // pattern: root.db1.d1.**, deviceId: root.db1.d1, match
        return pathPattern.getDevicePath().matchFullPath(deviceIdPath)
            // pattern: root.db1.**, deviceId: root.db1.d1, match
            || pathPattern.matchFullPath(deviceIdPath);
      } else {
        // pattern: root.db1.d1.s1, deviceId: root.db1.d1, match
        // pattern: root.db1.d1, deviceId: root.db1.d1, not match
        return pathPattern.getDevicePath().matchFullPath(new PartialPath(deviceID));
      }
    } catch (IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean affects(String measurementID) {
    return PathPatternUtil.isNodeMatch(pathPattern.getMeasurement(), measurementID);
  }

  @Override
  public boolean affectsAll(IDeviceID deviceID) {
    return pathPattern.matchFullPath(deviceID, AlignedPath.VECTOR_PLACEHOLDER);
  }

  @Override
  public PartialPath keyOfPatternTree() {
    return pathPattern;
  }

  @Override
  public ModEntry clone() {
    return new TreeDeletionEntry(this);
  }

  @Override
  public String toString() {
    return "TreeDeletionEntry{" + "pathPattern=" + pathPattern + ", timeRange=" + timeRange + '}';
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  @Override
  public int compareTo(ModEntry o) {
    if (this.getType() != o.getType()) {
      return Byte.compare(this.getType().getTypeNum(), o.getType().getTypeNum());
    }
    TreeDeletionEntry o1 = (TreeDeletionEntry) o;
    return Comparator.comparing(TreeDeletionEntry::getPathPattern)
        .thenComparing(TreeDeletionEntry::getTimeRange)
        .compare(this, o1);
  }

  public boolean intersects(TreeDeletionEntry deletion) {
    if (super.equals(deletion)) {
      return this.timeRange.intersects(deletion.getTimeRange());
    } else {
      return false;
    }
  }

  public void merge(TreeDeletionEntry deletion) {
    this.timeRange.merge(deletion.getTimeRange());
  }

  public long getSerializedSize() {
    return modType.getSerializedSize()
        + Integer.BYTES
        + (long) pathPattern.getFullPath().length() * Character.BYTES;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TreeDeletionEntry that = (TreeDeletionEntry) o;
    return Objects.equals(pathPattern, that.pathPattern)
        && Objects.equals(timeRange, that.timeRange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathPattern, timeRange);
  }
}
