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

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

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
  private static final int LONG_INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(Long.class);

  /**
   * Cache hit and the measurement is known to be null at the aligned last-row time. For stored
   * entries, it is only used as the value part of the time column's cached {@link TimeValuePair}.
   */
  public static final TsPrimitiveType PLACEHOLDER_NO_VALUE = new TsPrimitiveType.TsInt();

  /**
   * Cache hit but the target measurement is stale under a newer aligned last-row time. This
   * sentinel is only returned by {@link #getLastRow(String, List)} and is never stored in cache.
   */
  public static final TsPrimitiveType PLACEHOLDER_STALE_VALUE = new TsPrimitiveType.TsInt();

  private static final Optional<Pair<OptionalLong, TsPrimitiveType[]>> HIT_AND_ALL_NULL =
      Optional.of(new Pair<>(OptionalLong.empty(), null));

  /** This means the measurement has been cached and is known to have no values at all. */
  public static final TimeValuePair PLACEHOLDER_EMPTY_COLUMN =
      new TimeValuePair(Long.MIN_VALUE, PLACEHOLDER_NO_VALUE);

  /** This means that the tv pair has been declared, and is ready for the next put. */
  private static final TimeValuePair PLACEHOLDER_NO_CACHE =
      new TimeValuePair(Long.MIN_VALUE, PLACEHOLDER_NO_VALUE);

  // Time is seen as "" as a measurement
  private final Map<String, TimeValuePair> measurement2CachedLastMap = new ConcurrentHashMap<>();
  private final Map<String, Long> measurement2CachedLastKnownNullTimeMap =
      new ConcurrentHashMap<>();
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
      if (isInvalidate && measurement2CachedLastKnownNullTimeMap.remove(finalMeasurement) != null) {
        diff.addAndGet(-getKnownNullTimeEntrySize());
      }
      final TimeValuePair newPair = isInvalidate ? null : PLACEHOLDER_NO_CACHE;

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
    return tryUpdate(measurements, null, timeValuePairs, invalidateNull);
  }

  int tryUpdate(
      final @Nonnull String[] measurements,
      final @Nullable IMeasurementSchema[] measurementSchemas,
      final @Nonnull TimeValuePair[] timeValuePairs,
      final boolean invalidateNull) {
    final AtomicInteger diff = new AtomicInteger(0);
    long lastTime = Long.MIN_VALUE;

    for (int i = 0; i < measurements.length; ++i) {
      final String measurement = getRawMeasurement(measurements, measurementSchemas, i);
      if (Objects.isNull(measurement)) {
        continue;
      }
      if (Objects.isNull(timeValuePairs[i])) {
        if (invalidateNull) {
          diff.addAndGet(removeKnownNullTime(measurement));
          diff.addAndGet(
              -((int) RamUsageEstimator.sizeOf(measurement)
                  + getTvPairEntrySize(measurement2CachedLastMap.remove(measurement))));
        }
        continue;
      }

      if (isKnownNullAtAlignedTime(measurement, timeValuePairs[i])) {
        if (lastTime < timeValuePairs[i].getTimestamp()) {
          lastTime = timeValuePairs[i].getTimestamp();
        }
        diff.addAndGet(tryUpdateKnownNullTime(measurement, timeValuePairs[i].getTimestamp()));
        continue;
      }

      final int finalI = i;
      if (lastTime < timeValuePairs[i].getTimestamp()) {
        lastTime = timeValuePairs[i].getTimestamp();
      }
      measurement2CachedLastMap.computeIfPresent(
          measurement,
          (measurementName, tvPair) -> {
            if (tvPair.getTimestamp() <= timeValuePairs[finalI].getTimestamp()) {
              diff.addAndGet(
                  getDiffSize(tvPair, timeValuePairs[finalI])
                      + clearKnownNullTimeIfCovered(
                          measurementName, timeValuePairs[finalI].getTimestamp()));
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
                ? new TimeValuePair(finalLastTime, PLACEHOLDER_NO_VALUE)
                : tvPair);
    return diff.get();
  }

  @Nullable
  private static String getRawMeasurement(
      final @Nonnull String[] measurements,
      final @Nullable IMeasurementSchema[] measurementSchemas,
      final int index) {
    if (Objects.isNull(measurements[index])) {
      return null;
    }
    return Objects.nonNull(measurementSchemas)
            && index < measurementSchemas.length
            && Objects.nonNull(measurementSchemas[index])
        ? measurementSchemas[index].getMeasurementName()
        : measurements[index];
  }

  @GuardedBy("DataRegionInsertLock#writeLock")
  int invalidate(final String measurement) {
    final AtomicInteger diff = new AtomicInteger();
    final AtomicLong time = new AtomicLong();
    final AtomicLong knownNullTime = new AtomicLong(Long.MIN_VALUE);
    measurement2CachedLastMap.computeIfPresent(
        measurement,
        (s, timeValuePair) -> {
          diff.set(
              (isTableModel ? 0 : (int) RamUsageEstimator.sizeOf(s))
                  + getTvPairEntrySize(timeValuePair));
          time.set(timeValuePair.getTimestamp());
          return null;
        });
    final Long removedKnownNullTime = measurement2CachedLastKnownNullTimeMap.remove(measurement);
    if (removedKnownNullTime != null) {
      diff.addAndGet(-getKnownNullTimeEntrySize());
      knownNullTime.set(removedKnownNullTime);
    }
    if (diff.get() == 0) {
      return 0;
    }
    measurement2CachedLastMap.computeIfPresent(
        "",
        (s, timeValuePair) -> {
          if (timeValuePair.getTimestamp() <= Math.max(time.get(), knownNullTime.get())) {
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
        || tvPair == PLACEHOLDER_NO_CACHE
        || tvPair == PLACEHOLDER_EMPTY_COLUMN;
  }

  private static boolean isKnownNullAtAlignedTime(
      final @Nonnull String measurement, final @Nonnull TimeValuePair timeValuePair) {
    return !measurement.isEmpty()
        && timeValuePair != PLACEHOLDER_EMPTY_COLUMN
        && timeValuePair.getValue() == PLACEHOLDER_NO_VALUE;
  }

  @Nullable
  TimeValuePair getTimeValuePair(final @Nonnull String measurement) {
    final TimeValuePair result = measurement2CachedLastMap.get(measurement);
    return result != PLACEHOLDER_NO_CACHE ? result : null;
  }

  // Shall pass in "" if last by time
  Optional<Pair<OptionalLong, TsPrimitiveType[]>> getLastRow(
      final @Nonnull String sourceMeasurement, final List<String> targetMeasurements) {
    final TimeValuePair pair = measurement2CachedLastMap.get(sourceMeasurement);
    if (Objects.isNull(pair) || pair == PLACEHOLDER_NO_CACHE) {
      return Optional.empty();
    }

    if (pair == PLACEHOLDER_EMPTY_COLUMN) {
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
                        return getLastRowTargetValue(
                            alignTime,
                            measurement2CachedLastMap.get(targetMeasurement),
                            measurement2CachedLastKnownNullTimeMap.get(targetMeasurement));
                      } else {
                        return new TsPrimitiveType.TsLong(alignTime);
                      }
                    })
                .toArray(TsPrimitiveType[]::new)));
  }

  @Nullable
  private static TsPrimitiveType getLastRowTargetValue(
      final long alignTime,
      final @Nullable TimeValuePair tvPair,
      final @Nullable Long knownNullTime) {
    if (knownNullTime != null && knownNullTime == alignTime) {
      return PLACEHOLDER_NO_VALUE;
    }
    if (Objects.isNull(tvPair) || tvPair == PLACEHOLDER_NO_CACHE) {
      return null;
    }
    if (tvPair == PLACEHOLDER_EMPTY_COLUMN) {
      return PLACEHOLDER_NO_VALUE;
    }
    return tvPair.getTimestamp() == alignTime ? tvPair.getValue() : PLACEHOLDER_STALE_VALUE;
  }

  int estimateSize() {
    return INSTANCE_SIZE
        + (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY * measurement2CachedLastMap.size()
        + (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
            * measurement2CachedLastKnownNullTimeMap.size()
        + measurement2CachedLastMap.entrySet().stream()
            .mapToInt(
                entry ->
                    (isTableModel ? 0 : (int) RamUsageEstimator.sizeOf(entry.getKey()))
                        + TableDeviceLastCache.getTvPairSize(entry.getValue()))
            .reduce(0, Integer::sum)
        + measurement2CachedLastKnownNullTimeMap.size() * LONG_INSTANCE_SIZE;
  }

  private int tryUpdateKnownNullTime(final @Nonnull String measurement, final long knownNullTime) {
    final AtomicInteger diff = new AtomicInteger(0);
    measurement2CachedLastMap.computeIfPresent(
        measurement,
        (measurementName, tvPair) -> {
          measurement2CachedLastKnownNullTimeMap.compute(
              measurementName,
              (ignored, oldTime) -> {
                if (oldTime == null) {
                  diff.addAndGet(getKnownNullTimeEntrySize());
                  return knownNullTime;
                }
                return oldTime < knownNullTime ? knownNullTime : oldTime;
              });
          return tvPair;
        });
    return diff.get();
  }

  private int clearKnownNullTimeIfCovered(
      final @Nonnull String measurement, final long coveredTime) {
    if (measurement.isEmpty()) {
      return 0;
    }
    final Long knownNullTime = measurement2CachedLastKnownNullTimeMap.get(measurement);
    if (knownNullTime != null && knownNullTime <= coveredTime) {
      measurement2CachedLastKnownNullTimeMap.remove(measurement);
      return -getKnownNullTimeEntrySize();
    }
    return 0;
  }

  private int removeKnownNullTime(final @Nonnull String measurement) {
    return measurement2CachedLastKnownNullTimeMap.remove(measurement) == null
        ? 0
        : -getKnownNullTimeEntrySize();
  }

  private static int getKnownNullTimeEntrySize() {
    return (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + LONG_INSTANCE_SIZE;
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
