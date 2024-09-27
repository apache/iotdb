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

import org.apache.iotdb.db.utils.IOUtils.StreamSerializable;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ModEntry implements StreamSerializable {
  protected ModType modType;
  protected TimeRange timeRange;

  public ModEntry(ModType modType) {
    this.modType = modType;
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    stream.write(modType.getTypeNum());
    ReadWriteIOUtils.write(timeRange.getMin(), stream);
    ReadWriteIOUtils.write(timeRange.getMax(), stream);
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    this.timeRange =
        new TimeRange(ReadWriteIOUtils.readLong(stream), ReadWriteIOUtils.readLong(stream));
  }

  public static ModEntry createFrom(InputStream stream) throws IOException {
    ModType modType = ModType.deserialize(stream);
    ModEntry entry = modType.newEntry();
    entry.deserialize(stream);
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
}
