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

package org.apache.iotdb.db.subscription.event;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SubscriptionEventBinaryCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionEventBinaryCache.class);

  private final PipeMemoryBlock allocatedMemoryBlock;

  private final AtomicDouble memoryUsageCheatFactor = new AtomicDouble(1);

  private final LoadingCache<SubscriptionEvent, ByteBuffer> cache;

  public ByteBuffer serialize(final SubscriptionEvent event) throws IOException {
    try {
      return this.cache.get(event);
    } catch (final Exception e) {
      LOGGER.warn(
          "SubscriptionEventBinaryCache raised an exception while serializing SubscriptionEvent: {}",
          event,
          e);
      throw new IOException(e);
    }
  }

  /**
   * @return true -> byte buffer is not null
   */
  public boolean trySerialize(final SubscriptionEvent event) {
    try {
      serialize(event);
      return true;
    } catch (final IOException e) {
      LOGGER.warn(
          "Subscription: something unexpected happened when serializing SubscriptionEvent: {}",
          event,
          e);
      return false;
    }
  }

  public void resetByteBuffer(final SubscriptionEvent message, final boolean recursive) {
    message.resetByteBuffer(recursive);
    this.cache.invalidate(message);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionEventBinaryCacheHolder {

    private static final SubscriptionEventBinaryCache INSTANCE = new SubscriptionEventBinaryCache();

    private SubscriptionEventBinaryCacheHolder() {
      // empty constructor
    }
  }

  public static SubscriptionEventBinaryCache getInstance() {
    return SubscriptionEventBinaryCache.SubscriptionEventBinaryCacheHolder.INSTANCE;
  }

  private SubscriptionEventBinaryCache() {
    final long initMemorySizeInBytes =
        PipeResourceManager.memory().getTotalMemorySizeInBytes() / 10;
    final long maxMemorySizeInBytes =
        (long)
            (PipeResourceManager.memory().getTotalMemorySizeInBytes()
                * PipeConfig.getInstance().getSubscriptionCacheMemoryUsagePercentage());

    // properties required by pipe memory control framework
    this.allocatedMemoryBlock =
        PipeResourceManager.memory()
            .tryAllocate(initMemorySizeInBytes)
            .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 1))
            .setShrinkCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.set(
                      memoryUsageCheatFactor.get() * ((double) oldMemory / newMemory));
                  LOGGER.info(
                      "SubscriptionEventBinaryCache.allocatedMemoryBlock has shrunk from {} to {}.",
                      oldMemory,
                      newMemory);
                })
            .setExpandMethod(
                oldMemory -> Math.min(Math.max(oldMemory, 1) * 2, maxMemorySizeInBytes))
            .setExpandCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.set(
                      memoryUsageCheatFactor.get() / ((double) newMemory / oldMemory));
                  LOGGER.info(
                      "SubscriptionEventBinaryCache.allocatedMemoryBlock has expanded from {} to {}.",
                      oldMemory,
                      newMemory);
                });

    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(this.allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<SubscriptionEvent, ByteBuffer>)
                    (message, buffer) -> {
                      // TODO: overflow
                      return (int) (buffer.limit() * memoryUsageCheatFactor.get());
                    })
            .recordStats() // TODO: metrics
            .build(SubscriptionEvent::serialize);
  }
}
