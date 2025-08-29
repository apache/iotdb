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

package org.apache.iotdb.db.pipe.resource.log;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PipePeriodicalLogReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipePeriodicalLogReducer.class);
  private static final PipeMemoryBlock block;
  protected static final Cache<String, String> loggerCache;

  static {
    // Never close because it's static
    block =
        PipeDataNodeResourceManager.memory()
            .tryAllocate(PipeConfig.getInstance().getPipeLoggerCacheMaxSizeInBytes());
    loggerCache =
        Caffeine.newBuilder()
            .expireAfterWrite(
                PipeConfig.getInstance().getPipePeriodicalLogMinIntervalSeconds(), TimeUnit.SECONDS)
            .weigher(
                (k, v) ->
                    Math.toIntExact(
                        RamUsageEstimator.sizeOf((String) k)
                            + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY))
            .maximumWeight(block.getMemoryUsageInBytes())
            .build();
  }

  public static boolean log(
      final Consumer<String> loggerFunction, final String rawMessage, final Object... formatter) {
    final String loggerMessage = String.format(rawMessage, formatter);
    if (!loggerCache.asMap().containsKey(loggerMessage)) {
      loggerCache.put(loggerMessage, loggerMessage);
      loggerFunction.accept(loggerMessage);
      return true;
    }
    return false;
  }

  public static void update() {
    loggerCache
        .policy()
        .expireAfterWrite()
        .ifPresent(
            time ->
                time.setExpiresAfter(
                    PipeConfig.getInstance().getPipePeriodicalLogMinIntervalSeconds(),
                    TimeUnit.SECONDS));
    PipeDataNodeResourceManager.memory()
        .resize(block, PipeConfig.getInstance().getPipeLoggerCacheMaxSizeInBytes(), false);
    LOGGER.info(
        "PipePeriodicalLogReducer is allocated to {} bytes.", block.getMemoryUsageInBytes());
    loggerCache
        .policy()
        .eviction()
        .ifPresent(eviction -> eviction.setMaximum(block.getMemoryUsageInBytes()));
  }

  private PipePeriodicalLogReducer() {
    // static
  }
}
