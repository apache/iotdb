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

package org.apache.iotdb.commons.consensus.iotv2.consistency.merkle;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU cache for on-demand loading of .merkle file contents. Level 2+ Merkle data is loaded from
 * disk into this cache and evicted when memory pressure exceeds the configured maximum.
 */
public class MerkleFileCache {

  private final long maxMemoryBytes;
  private volatile long currentMemoryBytes;
  private final LinkedHashMap<String, MerkleFileContent> cache;
  private final ReentrantLock lock;

  public MerkleFileCache(long maxMemoryBytes) {
    this.maxMemoryBytes = maxMemoryBytes;
    this.currentMemoryBytes = 0;
    this.lock = new ReentrantLock();
    this.cache =
        new LinkedHashMap<String, MerkleFileContent>(64, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, MerkleFileContent> eldest) {
            if (currentMemoryBytes > maxMemoryBytes) {
              currentMemoryBytes -= eldest.getValue().estimatedMemoryBytes();
              return true;
            }
            return false;
          }
        };
  }

  /**
   * Get the .merkle content for a TsFile. Loads from disk if not cached.
   *
   * @param tsFilePath path to the TsFile (the .merkle file is at tsFilePath + ".merkle")
   * @return cached or freshly loaded content
   */
  public MerkleFileContent get(String tsFilePath) throws IOException {
    lock.lock();
    try {
      MerkleFileContent content = cache.get(tsFilePath);
      if (content != null) {
        return content;
      }
    } finally {
      lock.unlock();
    }

    MerkleFileContent loaded = MerkleFileReader.read(tsFilePath + ".merkle", tsFilePath);

    lock.lock();
    try {
      MerkleFileContent existing = cache.get(tsFilePath);
      if (existing != null) {
        return existing;
      }
      cache.put(tsFilePath, loaded);
      currentMemoryBytes += loaded.estimatedMemoryBytes();
      return loaded;
    } finally {
      lock.unlock();
    }
  }

  /** Invalidate a specific entry, e.g., when its TsFile is compacted away. */
  public void invalidate(String tsFilePath) {
    lock.lock();
    try {
      MerkleFileContent removed = cache.remove(tsFilePath);
      if (removed != null) {
        currentMemoryBytes -= removed.estimatedMemoryBytes();
      }
    } finally {
      lock.unlock();
    }
  }

  /** Clear all cached entries. */
  public void clear() {
    lock.lock();
    try {
      cache.clear();
      currentMemoryBytes = 0;
    } finally {
      lock.unlock();
    }
  }

  public long getCurrentMemoryBytes() {
    return currentMemoryBytes;
  }

  public int size() {
    lock.lock();
    try {
      return cache.size();
    } finally {
      lock.unlock();
    }
  }
}
