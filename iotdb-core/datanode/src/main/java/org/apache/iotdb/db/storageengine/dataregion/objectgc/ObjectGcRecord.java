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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One framed object-GC journal record. */
public class ObjectGcRecord {

  public static final byte TYPE_SCAN = 1;
  public static final byte TYPE_DROP_TABLE = 2;

  private byte type;
  private long taskId;
  private TableDeletionEntry deletion;

  /** partitionId -> exclusive upper bound (delete version &lt; bound, except protected). */
  private Map<Long, Long> exclusiveUpperByPartition = Collections.emptyMap();

  /** partitionId -> working TsFile versions that must not be unlinked by SCAN. */
  private Map<Long, Set<Long>> protectedVersionsByPartition = Collections.emptyMap();

  private List<String> dropTableDirs = Collections.emptyList();
  private long seq;
  private long endOffset;

  public static ObjectGcRecord scan(
      long taskId,
      TableDeletionEntry deletion,
      Map<Long, Long> exclusiveUpperByPartition,
      Map<Long, Set<Long>> protectedVersionsByPartition) {
    ObjectGcRecord record = new ObjectGcRecord();
    record.type = TYPE_SCAN;
    record.taskId = taskId;
    record.deletion = deletion;
    record.exclusiveUpperByPartition = exclusiveUpperByPartition;
    record.protectedVersionsByPartition = protectedVersionsByPartition;
    return record;
  }

  public static ObjectGcRecord dropTable(long taskId, List<String> dropTableDirs) {
    ObjectGcRecord record = new ObjectGcRecord();
    record.type = TYPE_DROP_TABLE;
    record.taskId = taskId;
    record.dropTableDirs = dropTableDirs;
    return record;
  }

  /**
   * Serialize one journal record. Capacity is an estimate; overflow doubles and retries.
   *
   * <pre>
   * SCAN (type=1):
   *   type:1 | taskId:8 | deletion | partitionCount:4
   *   ×N: partitionId:8 | exclusiveUpper:8 | protectedCount:4 | version:8×n
   *
   * DROP_TABLE (type=2):
   *   type:1 | taskId:8 | dirCount:4
   *   ×N: string = len:4 + UTF-8 bytes
   * </pre>
   *
   * Estimate: base = 1 + 8 + 256 // type + taskId + slack DROP_TABLE += 4 + dir.length()*3 // int
   * len + UTF-8 worst-case (3 B/char) SCAN += deletion.serializedSize() + 64 * partitionCount //
   * ~20 B fixed + a few protected versions
   */
  public ByteBuffer serialize() {
    int estimated = 1 + 8 + 256;
    if (type == TYPE_DROP_TABLE) {
      for (String dir : dropTableDirs) {
        estimated += 4 + dir.length() * 3;
      }
    } else if (deletion != null) {
      estimated += deletion.serializedSize() + 64 * exclusiveUpperByPartition.size();
    }
    ByteBuffer buffer = ByteBuffer.allocate(Math.max(estimated, 64));
    while (true) {
      int start = buffer.position();
      try {
        buffer.put(type);
        buffer.putLong(taskId);
        if (type == TYPE_SCAN) {
          deletion.serialize(buffer);
          buffer.putInt(exclusiveUpperByPartition.size());
          for (Map.Entry<Long, Long> entry : exclusiveUpperByPartition.entrySet()) {
            buffer.putLong(entry.getKey());
            buffer.putLong(entry.getValue());
            Set<Long> protectedVersions =
                protectedVersionsByPartition.getOrDefault(entry.getKey(), Collections.emptySet());
            buffer.putInt(protectedVersions.size());
            for (Long version : protectedVersions) {
              buffer.putLong(version);
            }
          }
        } else {
          buffer.putInt(dropTableDirs.size());
          for (String dir : dropTableDirs) {
            ReadWriteIOUtils.write(dir, buffer);
          }
        }
        return buffer;
      } catch (java.nio.BufferOverflowException e) {
        buffer = ByteBuffer.allocate(buffer.capacity() * 2);
        buffer.position(start);
      }
    }
  }

  public static ObjectGcRecord deserialize(ByteBuffer buffer) {
    ObjectGcRecord record = new ObjectGcRecord();
    record.type = buffer.get();
    record.taskId = buffer.getLong();
    if (record.type == TYPE_SCAN) {
      ModEntry entry = ModEntry.createFrom(buffer);
      record.deletion = (TableDeletionEntry) entry;
      int partitionCount = buffer.getInt();
      Map<Long, Long> exclusive = new HashMap<>(partitionCount);
      Map<Long, Set<Long>> protectedVersions = new HashMap<>(partitionCount);
      for (int i = 0; i < partitionCount; i++) {
        long partition = buffer.getLong();
        exclusive.put(partition, buffer.getLong());
        int n = buffer.getInt();
        Set<Long> versions = new HashSet<>(n);
        for (int j = 0; j < n; j++) {
          versions.add(buffer.getLong());
        }
        protectedVersions.put(partition, versions);
      }
      record.exclusiveUpperByPartition = exclusive;
      record.protectedVersionsByPartition = protectedVersions;
    } else if (record.type == TYPE_DROP_TABLE) {
      int n = buffer.getInt();
      List<String> dirs = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        dirs.add(ReadWriteIOUtils.readString(buffer));
      }
      record.dropTableDirs = dirs;
    }
    return record;
  }

  public boolean shouldDeleteVersion(long timePartition, long fileVersion) {
    Long exclusive = exclusiveUpperByPartition.get(timePartition);
    if (exclusive == null) {
      return false;
    }
    if (fileVersion >= exclusive) {
      return false;
    }
    Set<Long> protectedVersions = protectedVersionsByPartition.get(timePartition);
    return protectedVersions == null || !protectedVersions.contains(fileVersion);
  }

  public byte getType() {
    return type;
  }

  public long getTaskId() {
    return taskId;
  }

  public TableDeletionEntry getDeletion() {
    return deletion;
  }

  public List<String> getDropTableDirs() {
    return dropTableDirs;
  }

  public void setLocation(long seq, long endOffset) {
    this.seq = seq;
    this.endOffset = endOffset;
  }

  public long getSeq() {
    return seq;
  }

  public long getEndOffset() {
    return endOffset;
  }
}
