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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.read.filter.basic.Filter;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.math.LongMath.saturatedAdd;
import static org.apache.iotdb.commons.utils.CommonDateTimeUtils.saturateToLong;

public class TimePartitionUtils {
  private static final BigInteger BIG_LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger BIG_ONE = BigInteger.ONE;

  private static volatile DatabaseTimePartitionConfig defaultConfig =
      new DatabaseTimePartitionConfig(
          CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin(),
          CommonDescriptor.getInstance().getConfig().getTimePartitionInterval());
  private static final Map<String, DatabaseTimePartitionConfig> databaseConfigCache =
      new ConcurrentHashMap<>();

  // Update or add database-specific time partition configuration
  public static void updateDatabaseTimePartitionConfig(String database, TDatabaseSchema schema) {
    String timestampPrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    long interval =
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            schema.isSetTimePartitionInterval()
                ? schema.getTimePartitionInterval()
                : CommonDescriptor.getInstance().getConfig().getTimePartitionInterval(),
            timestampPrecision);
    long origin =
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            schema.isSetTimePartitionOrigin()
                ? schema.getTimePartitionOrigin()
                : CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin(),
            timestampPrecision);
    databaseConfigCache.put(database, new DatabaseTimePartitionConfig(origin, interval));
  }

  public static void updateDatabaseTimePartitionConfigs(Map<String, TDatabaseSchema> schemaMap) {
    if (schemaMap == null || schemaMap.isEmpty()) {
      return;
    }
    schemaMap.forEach(
        (database, schema) -> {
          if (database != null && schema != null) {
            updateDatabaseTimePartitionConfig(database, schema);
          }
        });
  }

  // Remove database-specific time partition configuration
  public static void removeDatabaseTimePartitionConfig(String database) {
    databaseConfigCache.remove(database);
  }

  public static void clearDatabaseTimePartitionConfigCache() {
    databaseConfigCache.clear();
  }

  public static boolean hasDatabaseTimePartitionConfig(String database) {
    return database != null && databaseConfigCache.containsKey(database);
  }

  public static TTimePartitionSlot getTimePartitionSlot(long time) {
    return getTimePartitionSlot(time, null);
  }

  public static TTimePartitionSlot getTimePartitionSlot(long time, String database) {
    return getDatabaseConfig(database).getTimePartitionSlot(time);
  }

  public static long getTimePartitionInterval() {
    return getTimePartitionInterval(null);
  }

  public static long getTimePartitionInterval(String database) {
    return getDatabaseConfig(database).getTimePartitionInterval();
  }

  public static long getTimePartitionLowerBound(long time) {
    return getTimePartitionLowerBound(time, null);
  }

  public static long getTimePartitionLowerBound(long time, String database) {
    return getDatabaseConfig(database).getTimePartitionLowerBound(time);
  }

  public static long getTimePartitionUpperBound(long time) {
    return getTimePartitionUpperBound(time, null);
  }

  public static long getTimePartitionUpperBound(long time, String database) {
    return getDatabaseConfig(database).getTimePartitionUpperBound(time);
  }

  public static long getTimePartitionEndTime(long time) {
    return getTimePartitionEndTime(time, null);
  }

  public static long getTimePartitionEndTime(long time, String database) {
    return getDatabaseConfig(database).getTimePartitionEndTime(time);
  }

  public static boolean isAfterOrEqualToTimePartitionUpperBound(
      long time, long timePartitionStartTime, long timePartitionUpperBound) {
    return isAfterOrEqualToTimePartitionUpperBound(
        time, timePartitionStartTime, timePartitionUpperBound, null);
  }

  public static boolean isAfterOrEqualToTimePartitionUpperBound(
      long time, long timePartitionStartTime, long timePartitionUpperBound, String database) {
    return getDatabaseConfig(database)
        .isAfterOrEqualToTimePartitionUpperBound(
            time, timePartitionStartTime, timePartitionUpperBound);
  }

  public static boolean isTimePartitionStartTime(long time) {
    return isTimePartitionStartTime(time, null);
  }

  public static boolean isTimePartitionStartTime(long time, String database) {
    return getDatabaseConfig(database).isTimePartitionStartTime(time);
  }

  public static long getTimePartitionId(long time) {
    return getTimePartitionId(time, null);
  }

  public static long getTimePartitionId(long time, String database) {
    return getDatabaseConfig(database).getTimePartitionId(time);
  }

  public static long getTimePartitionIdWithoutOverflow(long time) {
    return getTimePartitionIdWithoutOverflow(time, null);
  }

  public static long getTimePartitionIdWithoutOverflow(long time, String database) {
    return getDatabaseConfig(database).getTimePartitionIdWithoutOverflow(time);
  }

  public static long getStartTimeByPartitionId(long partitionId) {
    return getStartTimeByPartitionId(partitionId, null);
  }

  public static long getStartTimeByPartitionId(long partitionId, String database) {
    return getDatabaseConfig(database).getStartTimeByPartitionId(partitionId);
  }

  public static boolean satisfyPartitionId(long startTime, long endTime, long partitionId) {
    return satisfyPartitionId(startTime, endTime, partitionId, null);
  }

  public static boolean satisfyPartitionId(
      long startTime, long endTime, long partitionId, String database) {
    return getDatabaseConfig(database).satisfyPartitionId(startTime, endTime, partitionId);
  }

  public static boolean satisfyPartitionStartTime(Filter timeFilter, long partitionStartTime) {
    return satisfyPartitionStartTime(timeFilter, partitionStartTime, null);
  }

  public static boolean satisfyPartitionStartTime(
      Filter timeFilter, long partitionStartTime, String database) {
    return getDatabaseConfig(database).satisfyPartitionStartTime(timeFilter, partitionStartTime);
  }

  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId) {
    return satisfyTimePartition(timeFilter, partitionId, null);
  }

  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId, String database) {
    return getDatabaseConfig(database).satisfyTimePartition(timeFilter, partitionId);
  }

  public static long getEstimateTimePartitionSize(long startTime, long endTime) {
    return getEstimateTimePartitionSize(startTime, endTime, null);
  }

  public static long getEstimateTimePartitionSize(long startTime, long endTime, String database) {
    return getDatabaseConfig(database).getEstimateTimePartitionSize(startTime, endTime);
  }

  private static DatabaseTimePartitionConfig getDatabaseConfig(String database) {
    DatabaseTimePartitionConfig config =
        database == null ? null : databaseConfigCache.get(database);
    return config == null ? defaultConfig : config;
  }

  public static long getTimePartitionOrigin() {
    return defaultConfig.getTimePartitionOrigin();
  }

  public static long getTimePartitionOrigin(String database) {
    return getDatabaseConfig(database).getTimePartitionOrigin();
  }

  public static synchronized void setTimePartitionOrigin(long origin) {
    defaultConfig =
        new DatabaseTimePartitionConfig(origin, defaultConfig.getTimePartitionInterval());
  }

  public static synchronized void setTimePartitionInterval(long interval) {
    defaultConfig =
        new DatabaseTimePartitionConfig(defaultConfig.getTimePartitionOrigin(), interval);
  }

  public static class DatabaseTimePartitionConfig {
    private final long timePartitionOrigin;
    private final long timePartitionInterval;
    private long timePartitionLowerBoundWithoutOverflow;
    private long timePartitionUpperBoundWithoutOverflow;
    private boolean timePartitionLowerBoundOverflow;
    private boolean timePartitionUpperBoundOverflow;

    public DatabaseTimePartitionConfig(long origin, long interval) {
      this.timePartitionOrigin = origin;
      this.timePartitionInterval = interval;
      updateTimePartitionBound();
    }

    public long getTimePartitionOrigin() {
      return timePartitionOrigin;
    }

    private void updateTimePartitionBound() {
      BigInteger minPartition = getTimePartitionIdAsBigInteger(Long.MIN_VALUE);
      BigInteger maxPartition = getTimePartitionIdAsBigInteger(Long.MAX_VALUE);
      timePartitionLowerBoundOverflow = minPartition.compareTo(BIG_LONG_MIN) < 0;
      timePartitionUpperBoundOverflow = maxPartition.compareTo(BIG_LONG_MAX) > 0;

      // The lower/upper bounds are the starts of the first and last regular slots.  If the exact
      // partition id at a timestamp is outside the long id range, the corresponding boundary slot
      // is represented by the nearest long id and starts at the timestamp limit.
      BigInteger firstRepresentablePartition = minPartition.max(BIG_LONG_MIN);
      BigInteger firstRepresentableStart =
          getTimePartitionStartTimeAsBigInteger(firstRepresentablePartition);
      if (timePartitionLowerBoundOverflow || firstRepresentableStart.compareTo(BIG_LONG_MIN) < 0) {
        firstRepresentableStart =
            firstRepresentableStart.add(BigInteger.valueOf(timePartitionInterval));
      }
      timePartitionLowerBoundWithoutOverflow = saturateToLong(firstRepresentableStart);

      BigInteger lastRepresentablePartition = maxPartition.min(BIG_LONG_MAX);
      BigInteger lastRepresentableStart;
      if (timePartitionUpperBoundOverflow) {
        lastRepresentableStart =
            getTimePartitionStartTimeAsBigInteger(lastRepresentablePartition.add(BIG_ONE));
      } else {
        lastRepresentableStart = getTimePartitionStartTimeAsBigInteger(lastRepresentablePartition);
      }
      timePartitionUpperBoundWithoutOverflow = saturateToLong(lastRepresentableStart);
    }

    public TTimePartitionSlot getTimePartitionSlot(long time) {
      TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
      timePartitionSlot.setStartTime(getTimePartitionLowerBound(time));
      return timePartitionSlot;
    }

    public long getTimePartitionInterval() {
      return timePartitionInterval;
    }

    public long getTimePartitionLowerBound(long time) {
      if (time < timePartitionLowerBoundWithoutOverflow) {
        return Long.MIN_VALUE;
      }
      if (time >= timePartitionUpperBoundWithoutOverflow) {
        return timePartitionUpperBoundWithoutOverflow;
      }
      return getTimePartitionStartTime(getTimePartitionId(time));
    }

    public long getTimePartitionUpperBound(long time) {
      if (time >= timePartitionUpperBoundWithoutOverflow) {
        return Long.MAX_VALUE;
      }
      if (time < timePartitionLowerBoundWithoutOverflow) {
        return timePartitionLowerBoundWithoutOverflow;
      }
      return saturatedAdd(getTimePartitionLowerBound(time), timePartitionInterval);
    }

    public long getTimePartitionEndTime(long time) {
      long upperBound = getTimePartitionUpperBound(time);
      if (upperBound != Long.MAX_VALUE) {
        return upperBound - 1;
      }
      return getTimePartitionLowerBound(time) == getTimePartitionLowerBound(Long.MAX_VALUE)
          ? Long.MAX_VALUE
          : Long.MAX_VALUE - 1;
    }

    public boolean isAfterOrEqualToTimePartitionUpperBound(
        long time, long timePartitionStartTime, long timePartitionUpperBound) {
      if (timePartitionUpperBound != Long.MAX_VALUE) {
        return time >= timePartitionUpperBound;
      }
      return time == Long.MAX_VALUE && getTimePartitionLowerBound(time) != timePartitionStartTime;
    }

    public boolean isTimePartitionStartTime(long time) {
      return getTimePartitionLowerBound(time) == time;
    }

    public long getTimePartitionId(long time) {
      final long timeFromOrigin;
      try {
        timeFromOrigin = Math.subtractExact(time, timePartitionOrigin);
      } catch (ArithmeticException e) {
        return getTimePartitionIdWithoutOverflow(time);
      }
      return Math.floorDiv(timeFromOrigin, timePartitionInterval);
    }

    public long getTimePartitionIdWithoutOverflow(long time) {
      BigInteger partitionId = getTimePartitionIdAsBigInteger(time);
      if (partitionId.compareTo(BIG_LONG_MIN) < 0) {
        return Long.MIN_VALUE;
      }
      if (partitionId.compareTo(BIG_LONG_MAX) > 0) {
        return Long.MAX_VALUE;
      }
      return partitionId.longValue();
    }

    private BigInteger getTimePartitionIdAsBigInteger(long time) {
      BigInteger bigTime =
          BigInteger.valueOf(time).subtract(BigInteger.valueOf(timePartitionOrigin));
      BigInteger bigTimePartitionInterval = BigInteger.valueOf(timePartitionInterval);
      return bigTime.compareTo(BigInteger.ZERO) > 0
              || bigTime.remainder(bigTimePartitionInterval).equals(BigInteger.ZERO)
          ? bigTime.divide(bigTimePartitionInterval)
          : bigTime.divide(bigTimePartitionInterval).subtract(BigInteger.ONE);
    }

    public long getStartTimeByPartitionId(long partitionId) {
      return getTimePartitionStartTime(partitionId);
    }

    public boolean satisfyPartitionId(long startTime, long endTime, long partitionId) {
      long startPartition = getTimePartitionId(startTime);
      long endPartition = getTimePartitionId(endTime);
      return startPartition <= partitionId && endPartition >= partitionId;
    }

    public boolean satisfyPartitionStartTime(Filter timeFilter, long partitionStartTime) {
      if (timeFilter == null) {
        return true;
      }
      long partitionEndTime = getTimePartitionEndTime(partitionStartTime);
      return timeFilter.satisfyStartEndTime(partitionStartTime, partitionEndTime);
    }

    public boolean satisfyTimePartition(Filter timeFilter, long partitionId) {
      return satisfyPartitionStartTime(timeFilter, getTimePartitionStartTime(partitionId));
    }

    private long getTimePartitionStartTime(long partitionId) {
      // A clamped boundary id represents the saturated underflow/overflow partition, whose start is
      // the corresponding timestamp boundary rather than the mathematical start of that id.
      if (partitionId == Long.MIN_VALUE && timePartitionLowerBoundOverflow) {
        return Long.MIN_VALUE;
      }
      if (partitionId == Long.MAX_VALUE && timePartitionUpperBoundOverflow) {
        return timePartitionUpperBoundWithoutOverflow;
      }
      return saturateToLong(getTimePartitionStartTimeAsBigInteger(BigInteger.valueOf(partitionId)));
    }

    private BigInteger getTimePartitionStartTimeAsBigInteger(BigInteger partitionId) {
      return partitionId
          .multiply(BigInteger.valueOf(timePartitionInterval))
          .add(BigInteger.valueOf(timePartitionOrigin));
    }

    public long getEstimateTimePartitionSize(long startTime, long endTime) {
      BigInteger estimateSize =
          BigInteger.valueOf(endTime)
              .subtract(BigInteger.valueOf(startTime))
              .divide(BigInteger.valueOf(timePartitionInterval))
              .add(BigInteger.ONE);
      if (estimateSize.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
        return Long.MAX_VALUE;
      }
      return estimateSize.longValue();
    }
  }
}
