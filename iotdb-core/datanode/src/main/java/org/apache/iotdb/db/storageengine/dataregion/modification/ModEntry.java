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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.IOUtils.BufferSerializable;
import org.apache.iotdb.db.utils.IOUtils.StreamSerializable;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteIOUtils;

public abstract class ModEntry
    implements StreamSerializable, BufferSerializable, Comparable<ModEntry> {

  protected ModType modType;
  protected TimeRange timeRange;

  protected ModEntry(ModType modType) {
    this.modType = modType;
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    stream.write(modType.getTypeNum());
    ReadWriteIOUtils.write(timeRange.getMin(), stream);
    ReadWriteIOUtils.write(timeRange.getMax(), stream);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put(modType.getTypeNum());
    ReadWriteIOUtils.write(timeRange.getMin(), buffer);
    ReadWriteIOUtils.write(timeRange.getMax(), buffer);
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

  public abstract boolean matchesFull(PartialPath path);

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
