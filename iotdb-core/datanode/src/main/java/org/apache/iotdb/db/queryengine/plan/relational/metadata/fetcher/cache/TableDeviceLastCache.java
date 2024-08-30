/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.LastCacheContainer;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class TableDeviceLastCache {
  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TableDeviceLastCache.class);

  public static final TsPrimitiveType EMPTY_PRIMITIVE_TYPE =
      new TsPrimitiveType() {
        @Override
        public void setObject(Object o) {
          // Do nothing
        }

        @Override
        public void reset() {
          // Do nothing
        }

        @Override
        public int getSize() {
          return 0;
        }

        @Override
        public Object getValue() {
          return null;
        }

        @Override
        public String getStringValue() {
          return null;
        }

        @Override
        public TSDataType getDataType() {
          return null;
        }
      };

  private static final Optional<Pair<OptionalLong, TsPrimitiveType[]>> HIT_AND_ALL_NULL =
      Optional.of(new Pair<>(OptionalLong.empty(), null));
  public static final TimeValuePair EMPTY_TIME_VALUE_PAIR =
      new TimeValuePair(Long.MIN_VALUE, EMPTY_PRIMITIVE_TYPE);

  // Time is seen as "" as a measurement
  private final Map<String, TimeValuePair> measurement2CachedLastMap = new ConcurrentHashMap<>();

  int update(
      final @Nonnull String database,
      final @Nonnull String tableName,
      final String[] measurements,
      final TimeValuePair[] timeValuePairs) {
    final AtomicInteger diff = new AtomicInteger(0);

    for (int i = 0; i < measurements.length; ++i) {
      final int finalI = i;
      if (!measurement2CachedLastMap.containsKey(measurements[i])) {
        measurements[i] =
            DataNodeTableCache.getInstance()
                .tryGetInternColumnName(database, tableName, measurements[i]);
      }
      measurement2CachedLastMap.compute(
          measurements[i],
          (measurement, tvPair) -> {
            if (Objects.isNull(tvPair)
                || tvPair.getTimestamp() <= timeValuePairs[finalI].getTimestamp()) {
              diff.addAndGet(
                  Objects.nonNull(tvPair)
                      ? LastCacheContainer.getDiffSize(
                          tvPair.getValue(), timeValuePairs[finalI].getValue())
                      : (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                          + timeValuePairs[finalI].getSize());
              return timeValuePairs[finalI];
            }
            return tvPair;
          });
    }
    return diff.get();
  }

  int tryUpdate(final @Nonnull String[] measurements, final TimeValuePair[] timeValuePairs) {
    final AtomicInteger diff = new AtomicInteger(0);
    long lastTime = Long.MIN_VALUE;

    for (int i = 0; i < measurements.length; ++i) {
      if (Objects.isNull(timeValuePairs[i])) {
        continue;
      }
      final int finalI = i;
      if (lastTime < timeValuePairs[i].getTimestamp()) {
        lastTime = timeValuePairs[i].getTimestamp();
      }
      measurement2CachedLastMap.compute(
          measurements[i],
          (measurement, tvPair) -> {
            if (Objects.nonNull(tvPair)
                && tvPair.getTimestamp() <= timeValuePairs[finalI].getTimestamp()) {
              diff.addAndGet(
                  LastCacheContainer.getDiffSize(
                      tvPair.getValue(), timeValuePairs[finalI].getValue()));
              return timeValuePairs[finalI];
            }
            return tvPair;
          });
    }
    final long finalLastTime = lastTime;
    measurement2CachedLastMap.computeIfPresent(
        "",
        (time, tvPair) ->
            tvPair.getTimestamp() < finalLastTime
                ? new TimeValuePair(finalLastTime, null)
                : tvPair);
    return diff.get();
  }

  @Nullable
  TimeValuePair getTimeValuePair(final @Nonnull String measurement) {
    return measurement2CachedLastMap.get(measurement);
  }

  // Shall pass in "" if last by time
  Optional<Pair<OptionalLong, TsPrimitiveType[]>> getLastRow(
      final @Nonnull String sourceMeasurement, final List<String> targetMeasurements) {
    if (!measurement2CachedLastMap.containsKey(sourceMeasurement)) {
      return Optional.empty();
    }
    final TimeValuePair pair = measurement2CachedLastMap.get(sourceMeasurement);
    if (pair == EMPTY_TIME_VALUE_PAIR) {
      return HIT_AND_ALL_NULL;
    }
    final long alignTime = measurement2CachedLastMap.get(sourceMeasurement).getTimestamp();

    return Optional.of(
        new Pair<>(
            OptionalLong.of(alignTime),
            targetMeasurements.stream()
                .map(
                    targetMeasurement -> {
                      if (!targetMeasurement.isEmpty()) {
                        final TimeValuePair tvPair =
                            measurement2CachedLastMap.get(targetMeasurement);
                        if (Objects.isNull(tvPair)) {
                          return null;
                        }
                        return tvPair.getTimestamp() == alignTime
                            ? tvPair.getValue()
                            : EMPTY_PRIMITIVE_TYPE;
                      } else {
                        return new TsPrimitiveType.TsLong(alignTime);
                      }
                    })
                .toArray(TsPrimitiveType[]::new)));
  }

  int estimateSize() {
    return INSTANCE_SIZE
        + (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY * measurement2CachedLastMap.size()
        + measurement2CachedLastMap.values().stream()
            .mapToInt(TimeValuePair::getSize)
            .reduce(0, Integer::sum);
  }
}
