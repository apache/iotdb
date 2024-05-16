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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.AtomicDouble;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SubscriptionPolledMessageBinaryCache {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPolledMessageBinaryCache.class);

  private final PipeMemoryBlock allocatedMemoryBlock;

  private final AtomicDouble memoryUsageCheatFactor = new AtomicDouble(1);

  private final LoadingCache<SubscriptionPolledMessage, ByteBuffer> cache;

  public ByteBuffer serialize(final SubscriptionPolledMessage message) throws IOException {
    try {
      return this.cache.get(message);
    } catch (final Exception e) {
      LOGGER.warn(
          "SubscriptionPolledMessageBinaryCache raised an exception while serializing message: {}",
          message,
          e);
      throw new IOException(e);
    }
  }

  /**
   * @return true -> byte buffer is not null
   */
  public boolean trySerialize(final SubscriptionPolledMessage message) {
    try {
      serialize(message);
      return true;
    } catch (final IOException e) {
      LOGGER.warn(
          "Subscription: something unexpected happened when serializing SubscriptionPolledMessage",
          e);
      return false;
    }
  }

  public void resetByteBuffer(final SubscriptionPolledMessage message) {
    message.resetByteBuffer();
    this.cache.invalidate(message);
  }

  public void clearCache() {
    this.cache.invalidateAll();
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionPolledMessageBinaryCacheHolder {

    private static final SubscriptionPolledMessageBinaryCache INSTANCE =
        new SubscriptionPolledMessageBinaryCache();

    private SubscriptionPolledMessageBinaryCacheHolder() {
      // empty constructor
    }
  }

  public static SubscriptionPolledMessageBinaryCache getInstance() {
    return SubscriptionPolledMessageBinaryCache.SubscriptionPolledMessageBinaryCacheHolder.INSTANCE;
  }

  private SubscriptionPolledMessageBinaryCache() {
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
                      "SubscriptionPolledMessageBinaryCache.allocatedMemoryBlock has shrunk from {} to {}.",
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
                      "SubscriptionPolledMessageBinaryCache.allocatedMemoryBlock has expanded from {} to {}.",
                      oldMemory,
                      newMemory);
                });

    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(this.allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<SubscriptionPolledMessage, ByteBuffer>)
                    (message, buffer) -> {
                      return (int) (buffer.limit() * memoryUsageCheatFactor.get());
                    })
            .build(
                new CacheLoader<SubscriptionPolledMessage, ByteBuffer>() {
                  @Override
                  public @Nullable ByteBuffer load(
                      @NonNull final SubscriptionPolledMessage subscriptionPolledMessage)
                      throws IOException {
                    SubscriptionPolledMessage.serialize(subscriptionPolledMessage);
                    return subscriptionPolledMessage.getByteBuffer();
                  }
                });
  }
}
