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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
      first.forEach((device, group) -> firstMeasurements.put(device, group.getMeasurements()));
      Map<IDeviceID, List<Set<String>>> overlaps = new HashMap<>();
      while (snapshot.hasNext()) {
        for (var entry : snapshot.next().getMemTableMap().entrySet()) {
          Set<String> names = entry.getValue().getMeasurements();
          Set<String> previous = firstMeasurements.putIfAbsent(entry.getKey(), names);
          if (previous != null && !previous.containsAll(names)) {
            overlaps.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>()).add(names);
          }
        }
      }

      long count = 0;
      for (var entry : firstMeasurements.entrySet()) {
        List<Set<String>> others = overlaps.get(entry.getKey());
        if (others == null) {
          count += entry.getValue().size();
        } else {
          Set<String> unique = new HashSet<>(entry.getValue());
          others.forEach(unique::addAll);
          count += unique.size();
        }
      }
      return count;
    } finally {
      regionReadLock.unlock();
    }
  }
}
