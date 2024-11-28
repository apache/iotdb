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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.annotations.TreeModel;
import org.apache.iotdb.db.utils.io.BufferSerializable;
import org.apache.iotdb.db.utils.io.StreamSerializable;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ModEntry
    implements StreamSerializable, BufferSerializable, Comparable<ModEntry> {

  protected ModType modType;
  protected TimeRange timeRange;

  protected ModEntry(ModType modType) {
    this.modType = modType;
  }

  public int serializedSize() {
    // modType + time range
    return Byte.BYTES + Long.BYTES * 2 + Byte.BYTES * 2;
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    stream.write(modType.getTypeNum());
    long size = 1;
    size += ReadWriteIOUtils.write(timeRange.getMin(), stream);
    size += ReadWriteIOUtils.write(timeRange.getMax(), stream);
    return size;
  }

  @Override
  public long serialize(ByteBuffer buffer) {
    buffer.put(modType.getTypeNum());
    long size = 1;
    size += ReadWriteIOUtils.write(timeRange.getMin(), buffer);
    size += ReadWriteIOUtils.write(timeRange.getMax(), buffer);
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    this.timeRange =
        new TimeRange(ReadWriteIOUtils.readLong(stream), ReadWriteIOUtils.readLong(stream));
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    this.timeRange =
        new TimeRange(ReadWriteIOUtils.readLong(buffer), ReadWriteIOUtils.readLong(buffer));
  }

  public long getStartTime() {
    return timeRange.getMin();
  }

  public long getEndTime() {
    return timeRange.getMax();
  }

  /**
   * Test if a path can be matched by this modification.
   *
   * @param path a path to be matched.
   * @return true if the path can be matched by this modification, false otherwise.
   */
  @TreeModel
  @Deprecated
  public abstract boolean matches(PartialPath path);

  /**
   * Test if a device and associated time range can be affected by this modification. This deletion
   * does not necessarily delete the time column.
   */
  public abstract boolean affects(IDeviceID deviceID, long startTime, long endTime);

  /**
   * Test if a device can be affected by this modification. This deletion does not necessarily
   * delete the time column.
   */
  public abstract boolean affects(IDeviceID deviceID);

  /** Test if a measurement can be affected by this modification. */
  public abstract boolean affects(String measurementID);

  /** Test if the deletion affects all column (including the time column) of a device. */
  public abstract boolean affectsAll(IDeviceID deviceID);

  public abstract PartialPath keyOfPatternTree();

  public abstract ModEntry clone();

  public static ModEntry createFrom(InputStream stream) throws IOException {
    ModType modType = ModType.deserialize(stream);
    ModEntry entry = modType.newEntry();
    entry.deserialize(stream);
    return entry;
  }

  public static ModEntry createFrom(ByteBuffer buffer) {
    ModType modType = ModType.deserialize(buffer);
    ModEntry entry = modType.newEntry();
    entry.deserialize(buffer);
    return entry;
  }

  public enum ModType {
    TABLE_DELETION((byte) 0x00),
    TREE_DELETION((byte) 0x01);

    private final byte typeNum;

    ModType(byte typeNum) {
      this.typeNum = typeNum;
    }

    public byte getTypeNum() {
      return typeNum;
    }

    public long getSerializedSize() {
      return Byte.BYTES;
    }

    public ModEntry newEntry() {
      ModEntry entry;
      switch (this) {
        case TREE_DELETION:
          entry = new TreeDeletionEntry();
          break;
        case TABLE_DELETION:
          entry = new TableDeletionEntry();
          break;
        default:
          throw new IllegalArgumentException("Unsupported mod type: " + this);
      }
      return entry;
    }

    public static ModType deserialize(ByteBuffer buffer) {
      byte typeNum = buffer.get();
      switch (typeNum) {
        case 0x00:
          return TABLE_DELETION;
        case 0x01:
          return TREE_DELETION;
        default:
          throw new IllegalArgumentException("Unknown ModType: " + typeNum);
      }
    }

    public static ModType deserialize(InputStream stream) throws IOException {
      byte typeNum = ReadWriteIOUtils.readByte(stream);
      switch (typeNum) {
        case 0x00:
          return TABLE_DELETION;
        case 0x01:
          return TREE_DELETION;
        default:
          throw new IllegalArgumentException("Unknown ModType: " + typeNum);
      }
    }
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public ModType getType() {
    return modType;
  }
}
