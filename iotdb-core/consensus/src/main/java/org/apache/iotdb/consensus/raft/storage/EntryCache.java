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

package org.apache.iotdb.consensus.raft.storage;

import org.apache.iotdb.consensus.raft.core.log.LogEntry;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Byte-bounded LRU of recent entries (the paper's cache of size M); misses are counted. */
public final class EntryCache {

  private final long maxBytes;
  private final LinkedHashMap<Long, LogEntry> map = new LinkedHashMap<>(256, 0.75f, true);
  private long bytes;
  private long misses;

  public EntryCache(long maxBytes) {
    this.maxBytes = maxBytes;
  }

  public synchronized void put(LogEntry e) {
    LogEntry old = map.put(e.index, e);
    if (old != null) {
      bytes -= weight(old);
    }
    bytes += weight(e);
    evict();
  }

  public synchronized LogEntry get(long index) {
    LogEntry e = map.get(index);
    if (e == null) {
      misses++;
    }
    return e;
  }

  public synchronized void removeFrom(long from) {
    Iterator<Map.Entry<Long, LogEntry>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, LogEntry> en = it.next();
      if (en.getKey() >= from) {
        bytes -= weight(en.getValue());
        it.remove();
      }
    }
  }

  public synchronized void removeBefore(long index) {
    Iterator<Map.Entry<Long, LogEntry>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, LogEntry> en = it.next();
      if (en.getKey() < index) {
        bytes -= weight(en.getValue());
        it.remove();
      }
    }
  }

  public synchronized long misses() {
    return misses;
  }

  public synchronized int size() {
    return map.size();
  }

  public synchronized long bytes() {
    return bytes;
  }

  private static long weight(LogEntry e) {
    return 64L + e.size();
  }

  private void evict() {
    Iterator<Map.Entry<Long, LogEntry>> it = map.entrySet().iterator();
    while (bytes > maxBytes && it.hasNext()) {
      Map.Entry<Long, LogEntry> en = it.next();
      bytes -= weight(en.getValue());
      it.remove();
    }
  }
}
