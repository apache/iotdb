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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class TreeDeletionEntry extends ModEntry {

  private PartialPath pathPattern;

  public TreeDeletionEntry() {
    super(ModType.TREE_DELETION);
  }

  public TreeDeletionEntry(PartialPath path, long start, long end) {
    this(path, new TimeRange(start, end));
  }

  public TreeDeletionEntry(PartialPath path, TimeRange timeRange) {
    this();
    this.pathPattern = path;
    this.timeRange = timeRange;
  }

  public TreeDeletionEntry(PartialPath path, long endTime) {
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
  public void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.writeVar(pathPattern.getFullPath(), stream);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    super.serialize(buffer);
    ReadWriteIOUtils.writeVar(pathPattern.getFullPath(), buffer);
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    super.deserialize(stream);
    try {
      this.pathPattern = new PartialPath(ReadWriteIOUtils.readVarIntString(stream));
    } catch (IllegalPathException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    super.deserialize(buffer);
    try {
      this.pathPattern = new PartialPath(ReadWriteIOUtils.readVarIntString(buffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public boolean matchesFull(PartialPath path) {
    return pathPattern.matchFullPath(path);
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
}
