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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;

/** Counts distinct device/measurement pairs owned by a region's working and flushing MemTables. */
public final class ResidentSeriesCounter {
  private final Lock regionReadLock;
  private final CopyOnWriteArrayList<IMemTable> tables = new CopyOnWriteArrayList<>();

  public ResidentSeriesCounter(Lock regionReadLock) {
    this.regionReadLock = regionReadLock;
  }

  void add(IMemTable table) {
    tables.add(table);
  }

  void remove(IMemTable table) {
    tables.remove(table);
  }

  public long getSeriesCount() {
    // An empty registry already gives an exact count, even when a region writer holds the lock.
    if (tables.isEmpty()) {
      return 0;
    }
    // Working schemas can change under the region write lock. A flush may release a table
    // concurrently, but it does not alter its measurement indexes.
    regionReadLock.lock();
    try {
      var snapshot = tables.iterator();
      if (!snapshot.hasNext()) {
        return 0;
      }
      var first = snapshot.next().getMemTableMap();
      if (!snapshot.hasNext()) {
        long count = 0;
        for (IWritableMemChunkGroup group : first.values()) {
          count += group.getMeasurements().size();
        }
        return count;
      }

      Map<IDeviceID, Set<String>> firstMeasurements = new HashMap<>();
      Map<IDeviceID, Set<String>> additionalMeasurements = new HashMap<>();
      long count = 0;
      for (var entry : first.entrySet()) {
        Set<String> names = entry.getValue().getMeasurements();
        firstMeasurements.put(entry.getKey(), names);
        count += names.size();
      }
      while (snapshot.hasNext()) {
        for (var entry : snapshot.next().getMemTableMap().entrySet()) {
          IDeviceID device = entry.getKey();
          Set<String> names = entry.getValue().getMeasurements();
          Set<String> previous = firstMeasurements.putIfAbsent(device, names);
          if (previous == null) {
            count += names.size();
            continue;
          }
          Set<String> additional = additionalMeasurements.get(device);
          for (String name : names) {
            if (previous.contains(name)) {
              continue;
            }
            if (additional == null) {
              additional = new HashSet<>();
              additionalMeasurements.put(device, additional);
            }
            if (additional.add(name)) {
              count++;
            }
          }
        }
      }
      return count;
    } finally {
      regionReadLock.unlock();
    }
  }
}
