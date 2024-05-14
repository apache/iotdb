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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;

// TODO
public class EnrichedTabletsBinaryCache {

  private final PipeMemoryBlock allocatedMemoryBlock;

  private final LoadingCache<Long, Pair<ByteBuffer, EnrichedEvent>> cache;

  public EnrichedTabletsBinaryCache() {
    // TODO: config
    this.allocatedMemoryBlock =
        PipeResourceManager.memory().tryAllocate(Runtime.getRuntime().maxMemory() / 50);
    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(this.allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<Long, Pair<ByteBuffer, EnrichedEvent>>)
                    (id, enrichedTablets) -> enrichedTablets.left.limit())
            .build(
                new CacheLoader<Long, Pair<ByteBuffer, EnrichedEvent>>() {
                  @Override
                  public @Nullable Pair<ByteBuffer, EnrichedEvent> load(@NonNull Long aLong)
                      throws Exception {
                    return null;
                  }
                });
  }
}
