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

import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class TableDeviceLastCache {
  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TableDeviceLastCache.class)
          + (int) RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);

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

  /** This means that the tv pair has been put, and the value is null */
  public static final TimeValuePair EMPTY_TIME_VALUE_PAIR =
      new TimeValuePair(Long.MIN_VALUE, EMPTY_PRIMITIVE_TYPE);

  /** This means that the tv pair has been declared, and is ready for the next put. */
  private static final TimeValuePair PLACEHOLDER_TIME_VALUE_PAIR =
      new TimeValuePair(Long.MIN_VALUE, EMPTY_PRIMITIVE_TYPE);

  // Time is seen as "" as a measurement
  private final Map<String, TimeValuePair> measurement2CachedLastMap = new ConcurrentHashMap<>();
  private final boolean isTableModel;

  TableDeviceLastCache(final boolean isTableModel) {
    this.isTableModel = isTableModel;
  }

  int initOrInvalidate(
      final String database,
      final String tableName,
      final String[] measurements,
      final boolean isInvalidate) {
    final AtomicInteger diff = new AtomicInteger(0);

    for (final String measurement : measurements) {
      final String finalMeasurement =
          !measurement2CachedLastMap.containsKey(measurement) && isTableModel
              ? DataNodeTableCache.getInstance()
                  .tryGetInternColumnName(database, tableName, measurement)
              : measurement;

      // Removing table measurement, do not put cache
      if (Objects.isNull(finalMeasurement)) {
        continue;
      }
      final TimeValuePair newPair = isInvalidate ? null : PLACEHOLDER_TIME_VALUE_PAIR;

      measurement2CachedLastMap.compute(
          finalMeasurement,
          (measurementKey, tvPair) -> {
            if (Objects.isNull(newPair)) {
              diff.addAndGet(
                  -((isTableModel ? 0 : (int) RamUsageEstimator.sizeOf(finalMeasurement))
                      + getTvPairEntrySize(tvPair)));
              return null;
            }
            if (Objects.isNull(tvPair)) {
              diff.addAndGet(
                  (isTableModel ? 0 : (int) RamUsageEstimator.sizeOf(finalMeasurement))
                      + getTvPairEntrySize(newPair));
              return newPair;
            }
            return tvPair;
          });
    }
    return diff.get();
  }

  int tryUpdate(
      final @Nonnull String[] measurements, final @Nonnull TimeValuePair[] timeValuePairs) {
    return tryUpdate(measurements, timeValuePairs, false);
  }

  int tryUpdate(
      final @Nonnull String[] measurements,
      final @Nonnull TimeValuePair[] timeValuePairs,
      final boolean invalidateNull) {
    final AtomicInteger diff = new AtomicInteger(0);
    long lastTime = Long.MIN_VALUE;

    for (int i = 0; i < measurements.length; ++i) {
      if (Objects.isNull(timeValuePairs[i])) {
        if (invalidateNull) {
          diff.addAndGet(
              -((int) RamUsageEstimator.sizeOf(measurements[i])
                  + getTvPairEntrySize(measurement2CachedLastMap.remove(measurements[i]))));
        }
        continue;
      }

      final int finalI = i;
      if (lastTime < timeValuePairs[i].getTimestamp()) {
        lastTime = timeValuePairs[i].getTimestamp();
      }
      measurement2CachedLastMap.computeIfPresent(
          measurements[i],
          (measurement, tvPair) -> {
            if (tvPair.getTimestamp() <= timeValuePairs[finalI].getTimestamp()) {
              diff.addAndGet(getDiffSize(tvPair, timeValuePairs[finalI]));
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
                ? new TimeValuePair(finalLastTime, EMPTY_PRIMITIVE_TYPE)
                : tvPair);
    return diff.get();
  }

  @GuardedBy("DataRegionInsertLock#writeLock")
  int invalidate(final String measurement) {
    final AtomicInteger diff = new AtomicInteger();
    final AtomicLong time = new AtomicLong();
    measurement2CachedLastMap.computeIfPresent(
        measurement,
        (s, timeValuePair) -> {
          diff.set(
              (isTableModel ? 0 : (int) RamUsageEstimator.sizeOf(s))
                  + getTvPairEntrySize(timeValuePair));
          time.set(timeValuePair.getTimestamp());
          return null;
        });
    if (diff.get() == 0) {
      return 0;
    }
    measurement2CachedLastMap.computeIfPresent(
        "",
        (s, timeValuePair) -> {
          if (timeValuePair.getTimestamp() <= time.get()) {
            diff.addAndGet((int) RamUsageEstimator.sizeOf(s) + getTvPairEntrySize(timeValuePair));
            return null;
          }
          return timeValuePair;
        });

    return diff.get();
  }

  private static int getTvPairEntrySize(final TimeValuePair tvPair) {
    return (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + getTvPairSize(tvPair);
  }

  private static int getTvPairSize(final TimeValuePair tvPair) {
    return isEmptyTvPair(tvPair) ? 0 : tvPair.getSize();
  }

  private static boolean isEmptyTvPair(final TimeValuePair tvPair) {
    return Objects.isNull(tvPair)
        || tvPair == PLACEHOLDER_TIME_VALUE_PAIR
        || tvPair == EMPTY_TIME_VALUE_PAIR;
  }

  @Nullable
  TimeValuePair getTimeValuePair(final @Nonnull String measurement) {
    final TimeValuePair result = measurement2CachedLastMap.get(measurement);
    return result != PLACEHOLDER_TIME_VALUE_PAIR ? result : null;
  }

  // Shall pass in "" if last by time
  Optional<Pair<OptionalLong, TsPrimitiveType[]>> getLastRow(
      final @Nonnull String sourceMeasurement, final List<String> targetMeasurements) {
    final TimeValuePair pair = measurement2CachedLastMap.get(sourceMeasurement);
    if (Objects.isNull(pair) || pair == PLACEHOLDER_TIME_VALUE_PAIR) {
      return Optional.empty();
    }

    if (pair == EMPTY_TIME_VALUE_PAIR) {
      return HIT_AND_ALL_NULL;
    }
    final long alignTime = pair.getTimestamp();

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
        + measurement2CachedLastMap.entrySet().stream()
            .mapToInt(
                entry ->
                    (isTableModel ? 0 : (int) RamUsageEstimator.sizeOf(entry.getKey()))
                        + TableDeviceLastCache.getTvPairSize(entry.getValue()))
            .reduce(0, Integer::sum);
  }

  private static int getDiffSize(
      final TimeValuePair oldTimeValuePair, final TimeValuePair newTimeValuePair) {
    if (isEmptyTvPair(oldTimeValuePair)) {
      return getTvPairSize(newTimeValuePair);
    }
    if (isEmptyTvPair(newTimeValuePair)) {
      return -getTvPairSize(oldTimeValuePair);
    }
    final TsPrimitiveType oldValue = oldTimeValuePair.getValue();
    final TsPrimitiveType newValue = newTimeValuePair.getValue();
    if (oldValue == null) {
      return newValue == null ? 0 : newValue.getSize();
    } else if (oldValue instanceof TsPrimitiveType.TsBinary) {
      return (newValue == null ? 0 : newValue.getSize()) - oldValue.getSize();
    } else {
      return newValue == null ? -oldValue.getSize() : 0;
    }
  }
}
