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

package org.apache.iotdb.db.subscription.event.cache;

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * This class is used to control memory usage of cache {@link SubscriptionPollResponse} in {@link
 * CachedSubscriptionPollResponse}.
 */
public class SubscriptionPollResponseCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPollResponseCache.class);

  private final AtomicDouble memoryUsageCheatFactor = new AtomicDouble(1);

  private final LoadingCache<CachedSubscriptionPollResponse, ByteBuffer> cache;

  public ByteBuffer serialize(final CachedSubscriptionPollResponse response) throws IOException {
    try {
      return this.cache.get(response);
    } catch (final Exception e) {
      LOGGER.warn(
          "SubscriptionEventBinaryCache raised an exception while serializing CachedSubscriptionPollResponse: {}",
          response,
          e);
      throw new IOException(e);
    }
  }

  public Optional<ByteBuffer> trySerialize(final CachedSubscriptionPollResponse response) {
    try {
      return Optional.of(serialize(response));
    } catch (final IOException e) {
      LOGGER.warn(
          "Subscription: something unexpected happened when serializing CachedSubscriptionPollResponse: {}",
          response,
          e);
      return Optional.empty();
    }
  }

  public void invalidate(final CachedSubscriptionPollResponse response) {
    this.cache.invalidate(response);
    response.invalidateByteBuffer();
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionEventBinaryCacheHolder {

    private static final SubscriptionPollResponseCache INSTANCE =
        new SubscriptionPollResponseCache();

    private SubscriptionEventBinaryCacheHolder() {
      // empty constructor
    }
  }

  public static SubscriptionPollResponseCache getInstance() {
    return SubscriptionPollResponseCache.SubscriptionEventBinaryCacheHolder.INSTANCE;
  }

  private SubscriptionPollResponseCache() {
    final long initMemorySizeInBytes =
        PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes() / 5;
    final long maxMemorySizeInBytes =
        (long)
            (PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes()
                * SubscriptionConfig.getInstance().getSubscriptionCacheMemoryUsagePercentage());

    // properties required by pipe memory control framework
    final PipeMemoryBlock allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .tryAllocate(initMemorySizeInBytes)
            .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 1))
            .setShrinkCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.updateAndGet(
                      factor -> factor * ((double) oldMemory / newMemory));
                  LOGGER.info(
                      "SubscriptionEventBinaryCache.allocatedMemoryBlock has shrunk from {} to {}.",
                      oldMemory,
                      newMemory);
                })
            .setExpandMethod(
                oldMemory -> Math.min(Math.max(oldMemory, 1) * 2, maxMemorySizeInBytes))
            .setExpandCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.updateAndGet(
                      factor -> factor / ((double) newMemory / oldMemory));
                  LOGGER.info(
                      "SubscriptionEventBinaryCache.allocatedMemoryBlock has expanded from {} to {}.",
                      oldMemory,
                      newMemory);
                });

    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<CachedSubscriptionPollResponse, ByteBuffer>)
                    (message, buffer) -> {
                      // TODO: overflow
                      return (int) (buffer.capacity() * memoryUsageCheatFactor.get());
                    })
            .recordStats() // TODO: metrics
            // NOTE: lambda CAN NOT be replaced with method reference
            .build(response -> CachedSubscriptionPollResponse.serialize(response));
  }
}
