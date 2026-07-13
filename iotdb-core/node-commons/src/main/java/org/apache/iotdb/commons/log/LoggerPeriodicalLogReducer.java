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

package org.apache.iotdb.commons.log;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.CommonMessages;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

public class LoggerPeriodicalLogReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggerPeriodicalLogReducer.class);

  private static final LongUnaryOperator DEFAULT_MEMORY_RESIZE_FUNCTION =
      sizeInBytes -> sizeInBytes;

  private static volatile LongUnaryOperator memoryResizeFunction = DEFAULT_MEMORY_RESIZE_FUNCTION;

  protected static final Cache<String, String> LOGGER_CACHE =
      Caffeine.newBuilder()
          .expireAfterWrite(getLogMinIntervalSeconds(), TimeUnit.SECONDS)
          .weigher(LoggerPeriodicalLogReducer::estimateSize)
          .maximumWeight(getLoggerCacheMaxSizeInBytes())
          .build();

  private static int estimateSize(final String key, final String value) {
    return Math.toIntExact(
        RamUsageEstimator.sizeOf(key) + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
  }

  public static boolean log(
      final Consumer<String> loggerFunction, final String rawMessage, final Object... formatter) {
    final String loggerMessage = formatMessage(rawMessage, formatter);
    if (shouldLog(loggerMessage)) {
      loggerFunction.accept(loggerMessage);
      return true;
    }
    return false;
  }

  public static boolean shouldLog(final String loggerMessage) {
    return LOGGER_CACHE.asMap().putIfAbsent(loggerMessage, loggerMessage) == null;
  }

  public static boolean shouldLog(final String rawMessage, final Object... formatter) {
    return shouldLog(formatMessage(rawMessage, formatter));
  }

  public static synchronized void setMemoryResizeFunction(
      final LongUnaryOperator memoryResizeFunction) {
    LoggerPeriodicalLogReducer.memoryResizeFunction =
        memoryResizeFunction == null ? DEFAULT_MEMORY_RESIZE_FUNCTION : memoryResizeFunction;
    update();
  }

  public static synchronized void update() {
    final long maxWeight = memoryResizeFunction.applyAsLong(getLoggerCacheMaxSizeInBytes());
    LOGGER.info(
        CommonMessages.LOG_LOGGERPERIODICALLOGREDUCER_IS_ALLOCATED_TO_ARG_BYTES_C8373CF5,
        maxWeight);
    update(maxWeight);
  }

  public static synchronized void update(final long maxWeight) {
    LOGGER_CACHE
        .policy()
        .expireAfterWrite()
        .ifPresent(time -> time.setExpiresAfter(getLogMinIntervalSeconds(), TimeUnit.SECONDS));
    LOGGER_CACHE.policy().eviction().ifPresent(eviction -> eviction.setMaximum(maxWeight));
  }

  public static String formatMessage(final String rawMessage, final Object... formatter) {
    if (formatter == null || formatter.length == 0) {
      return rawMessage;
    }
    if (rawMessage.contains("{}")) {
      return MessageFormatter.arrayFormat(rawMessage, formatter).getMessage();
    }
    return String.format(rawMessage, formatter);
  }

  private static long getLogMinIntervalSeconds() {
    return CommonDescriptor.getInstance().getConfig().getLoggerPeriodicalLogMinIntervalSeconds();
  }

  private static long getLoggerCacheMaxSizeInBytes() {
    return CommonDescriptor.getInstance().getConfig().getLoggerCacheMaxSizeInBytes();
  }

  private LoggerPeriodicalLogReducer() {
    // static
  }
}
