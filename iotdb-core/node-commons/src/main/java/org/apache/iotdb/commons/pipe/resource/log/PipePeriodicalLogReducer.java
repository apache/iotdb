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

package org.apache.iotdb.commons.pipe.resource.log;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

public class PipePeriodicalLogReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipePeriodicalLogReducer.class);

  private static final LongUnaryOperator DEFAULT_MEMORY_RESIZE_FUNCTION =
      sizeInBytes -> sizeInBytes;

  private static volatile LongUnaryOperator memoryResizeFunction = DEFAULT_MEMORY_RESIZE_FUNCTION;

  protected static final Cache<String, String> LOGGER_CACHE =
      Caffeine.newBuilder()
          .expireAfterWrite(
              PipeConfig.getInstance().getPipePeriodicalLogMinIntervalSeconds(), TimeUnit.SECONDS)
          .weigher(PipePeriodicalLogReducer::estimateSize)
          .maximumWeight(PipeConfig.getInstance().getPipeLoggerCacheMaxSizeInBytes())
          .build();

  private static int estimateSize(final String key, final String value) {
    return Math.toIntExact(
        RamUsageEstimator.sizeOf(key) + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
  }

  public static boolean log(
      final Consumer<String> loggerFunction, final String rawMessage, final Object... formatter) {
    final String loggerMessage = String.format(rawMessage, formatter);
    if (!LOGGER_CACHE.asMap().containsKey(loggerMessage)) {
      LOGGER_CACHE.put(loggerMessage, loggerMessage);
      loggerFunction.accept(loggerMessage);
      return true;
    }
    return false;
  }

  public static synchronized void setMemoryResizeFunction(
      final LongUnaryOperator memoryResizeFunction) {
    PipePeriodicalLogReducer.memoryResizeFunction =
        memoryResizeFunction == null ? DEFAULT_MEMORY_RESIZE_FUNCTION : memoryResizeFunction;
    update();
  }

  public static synchronized void update() {
    final long maxWeight =
        memoryResizeFunction.applyAsLong(
            PipeConfig.getInstance().getPipeLoggerCacheMaxSizeInBytes());
    LOGGER.info("PipePeriodicalLogReducer is allocated to {} bytes.", maxWeight);
    update(maxWeight);
  }

  public static synchronized void update(final long maxWeight) {
    LOGGER_CACHE
        .policy()
        .expireAfterWrite()
        .ifPresent(
            time ->
                time.setExpiresAfter(
                    PipeConfig.getInstance().getPipePeriodicalLogMinIntervalSeconds(),
                    TimeUnit.SECONDS));
    LOGGER_CACHE.policy().eviction().ifPresent(eviction -> eviction.setMaximum(maxWeight));
  }

  private PipePeriodicalLogReducer() {
    // static
  }
}
