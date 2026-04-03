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

public class TimePartitionUtils {

  /**
   * Time partition origin for dividing database, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  private static long timePartitionOrigin =
      CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();

  /** Time range for dividing database, the time unit is the same with IoTDB's TimestampPrecision */
  private static long timePartitionInterval =
      CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

  // Database-specific time partition settings cache
  private static final Map<String, DatabaseTimePartitionConfig> databaseConfigCache =
      new ConcurrentHashMap<>();

  private static final BigInteger bigTimePartitionOrigin = BigInteger.valueOf(timePartitionOrigin);
  private static final BigInteger bigTimePartitionInterval =
      BigInteger.valueOf(timePartitionInterval);
  private static final boolean originMayCauseOverflow = (timePartitionOrigin != 0);
  private static final long timePartitionLowerBoundWithoutOverflow;
  private static final long timePartitionUpperBoundWithoutOverflow;

  static {
    long minPartition =
        getTimePartitionIdWithoutOverflow(
            Long.MIN_VALUE, timePartitionOrigin, timePartitionInterval);
    long maxPartition =
        getTimePartitionIdWithoutOverflow(
            Long.MAX_VALUE, timePartitionOrigin, timePartitionInterval);
    BigInteger minPartitionStartTime =
        BigInteger.valueOf(minPartition)
            .multiply(bigTimePartitionInterval)
            .add(bigTimePartitionOrigin);
    BigInteger maxPartitionEndTime =
        BigInteger.valueOf(maxPartition)
            .multiply(bigTimePartitionInterval)
            .add(bigTimePartitionInterval)
            .add(bigTimePartitionOrigin);
    if (minPartitionStartTime.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
      timePartitionLowerBoundWithoutOverflow =
          minPartitionStartTime.add(bigTimePartitionInterval).longValue();
    } else {
      timePartitionLowerBoundWithoutOverflow = minPartitionStartTime.longValue();
    }
    if (maxPartitionEndTime.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      timePartitionUpperBoundWithoutOverflow =
          maxPartitionEndTime.subtract(bigTimePartitionInterval).longValue();
    } else {
      timePartitionUpperBoundWithoutOverflow = maxPartitionEndTime.longValue();
    }
  }

  // Database-specific time partition configuration class
  public static class DatabaseTimePartitionConfig {
    private final long timePartitionOrigin;
    private final long timePartitionInterval;
    private final BigInteger bigTimePartitionOrigin;
    private final BigInteger bigTimePartitionInterval;
    private final boolean originMayCauseOverflow;
    private final long timePartitionLowerBoundWithoutOverflow;
    private final long timePartitionUpperBoundWithoutOverflow;

    public DatabaseTimePartitionConfig(long timePartitionOrigin, long timePartitionInterval) {
      this.timePartitionOrigin = timePartitionOrigin;
      this.timePartitionInterval = timePartitionInterval;
      this.bigTimePartitionOrigin = BigInteger.valueOf(timePartitionOrigin);
      this.bigTimePartitionInterval = BigInteger.valueOf(timePartitionInterval);
      this.originMayCauseOverflow = (timePartitionOrigin != 0);

      // Calculate bounds for overflow handling
      long minPartition =
          getTimePartitionIdWithoutOverflow(
              Long.MIN_VALUE, timePartitionOrigin, timePartitionInterval);
      long maxPartition =
          getTimePartitionIdWithoutOverflow(
              Long.MAX_VALUE, timePartitionOrigin, timePartitionInterval);
      BigInteger minPartitionStartTime =
          BigInteger.valueOf(minPartition)
              .multiply(this.bigTimePartitionInterval)
              .add(this.bigTimePartitionOrigin);
      BigInteger maxPartitionEndTime =
          BigInteger.valueOf(maxPartition)
              .multiply(this.bigTimePartitionInterval)
              .add(this.bigTimePartitionInterval)
              .add(this.bigTimePartitionOrigin);
      if (minPartitionStartTime.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
        this.timePartitionLowerBoundWithoutOverflow =
            minPartitionStartTime.add(this.bigTimePartitionInterval).longValue();
      } else {
        this.timePartitionLowerBoundWithoutOverflow = minPartitionStartTime.longValue();
      }
      if (maxPartitionEndTime.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
        this.timePartitionUpperBoundWithoutOverflow =
            maxPartitionEndTime.subtract(this.bigTimePartitionInterval).longValue();
      } else {
        this.timePartitionUpperBoundWithoutOverflow = maxPartitionEndTime.longValue();
      }
    }

    // Getters for database-specific configuration
    public long getTimePartitionOrigin() {
      return timePartitionOrigin;
    }

    public long getTimePartitionInterval() {
      return timePartitionInterval;
    }

    public boolean isOriginMayCauseOverflow() {
      return originMayCauseOverflow;
    }

    public long getTimePartitionLowerBoundWithoutOverflow() {
      return timePartitionLowerBoundWithoutOverflow;
    }

    public long getTimePartitionUpperBoundWithoutOverflow() {
      return timePartitionUpperBoundWithoutOverflow;
    }

    public BigInteger getBigTimePartitionOrigin() {
      return bigTimePartitionOrigin;
    }

    public BigInteger getBigTimePartitionInterval() {
      return bigTimePartitionInterval;
    }
  }

  // Update or add database-specific time partition configuration
  public static void updateDatabaseTimePartitionConfig(String database, TDatabaseSchema schema) {
    long interval =
        schema.isSetTimePartitionInterval()
            ? schema.getTimePartitionInterval()
            : timePartitionInterval;
    long origin =
        schema.isSetTimePartitionOrigin() ? schema.getTimePartitionOrigin() : timePartitionOrigin;
    databaseConfigCache.put(database, new DatabaseTimePartitionConfig(origin, interval));
  }

  // Remove database-specific time partition configuration
  public static void removeDatabaseTimePartitionConfig(String database) {
    databaseConfigCache.remove(database);
  }

  // Get database-specific configuration, fallback to global if not found
  private static DatabaseTimePartitionConfig getDatabaseConfig(String database) {
    if (database == null) {
      return new DatabaseTimePartitionConfig(timePartitionOrigin, timePartitionInterval);
    }
    DatabaseTimePartitionConfig config = databaseConfigCache.get(database);
    return config != null
        ? config
        : new DatabaseTimePartitionConfig(timePartitionOrigin, timePartitionInterval);
  }

  // Database-specific time partition methods
  public static TTimePartitionSlot getTimePartitionSlot(long time, String database) {
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
    timePartitionSlot.setStartTime(getTimePartitionLowerBoundInternal(time, config));
    return timePartitionSlot;
  }

  public static long getTimePartitionInterval(String database) {
    return getDatabaseConfig(database).getTimePartitionInterval();
  }

  public static long getTimePartitionOrigin(String database) {
    return getDatabaseConfig(database).getTimePartitionOrigin();
  }

  public static long getTimePartitionLowerBound(long time, String database) {
    return getTimePartitionLowerBoundInternal(time, getDatabaseConfig(database));
  }

  public static long getTimePartitionUpperBound(long time, String database) {
    return getTimePartitionUpperBoundInternal(time, getDatabaseConfig(database));
  }

  public static long getTimePartitionId(long time, String database) {
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    time -= config.getTimePartitionOrigin();
    return time > 0 || time % config.getTimePartitionInterval() == 0
        ? time / config.getTimePartitionInterval()
        : time / config.getTimePartitionInterval() - 1;
  }

  public static long getStartTimeByPartitionId(long partitionId, String database) {
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    return (partitionId * config.getTimePartitionInterval()) + config.getTimePartitionOrigin();
  }

  public static boolean satisfyPartitionId(
      long startTime, long endTime, long partitionId, String database) {
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    long startPartition =
        config.isOriginMayCauseOverflow()
            ? getTimePartitionIdWithoutOverflow(
                startTime, config.getTimePartitionOrigin(), config.getTimePartitionInterval())
            : getTimePartitionId(
                startTime, config.getTimePartitionOrigin(), config.getTimePartitionInterval());
    long endPartition =
        config.isOriginMayCauseOverflow()
            ? getTimePartitionIdWithoutOverflow(
                endTime, config.getTimePartitionOrigin(), config.getTimePartitionInterval())
            : getTimePartitionId(
                endTime, config.getTimePartitionOrigin(), config.getTimePartitionInterval());
    return startPartition <= partitionId && endPartition >= partitionId;
  }

  public static boolean satisfyPartitionStartTime(
      Filter timeFilter, long partitionStartTime, String database) {
    if (timeFilter == null) {
      return true;
    }
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    long partitionEndTime =
        partitionStartTime >= config.getTimePartitionLowerBoundWithoutOverflow()
            ? Long.MAX_VALUE
            : (partitionStartTime + config.getTimePartitionInterval() - 1);
    return timeFilter.satisfyStartEndTime(partitionStartTime, partitionEndTime);
  }

  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId, String database) {
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    long partitionStartTime;
    if (config.isOriginMayCauseOverflow()) {
      partitionStartTime =
          BigInteger.valueOf(partitionId)
              .multiply(config.getBigTimePartitionInterval())
              .add(config.getBigTimePartitionOrigin())
              .longValue();
    } else {
      partitionStartTime =
          partitionId * config.getTimePartitionInterval() + config.getTimePartitionOrigin();
    }
    return satisfyPartitionStartTime(timeFilter, partitionStartTime, database);
  }

  public static long getEstimateTimePartitionSize(long startTime, long endTime, String database) {
    DatabaseTimePartitionConfig config = getDatabaseConfig(database);
    if (endTime > 0 && startTime < 0) {
      return BigInteger.valueOf(endTime)
              .subtract(BigInteger.valueOf(startTime))
              .divide(config.getBigTimePartitionInterval())
              .longValue()
          + 1;
    }
    return (endTime - startTime) / config.getTimePartitionInterval() + 1;
  }

  // Helper methods for database-specific calculations
  private static long getTimePartitionLowerBoundInternal(
      long time, DatabaseTimePartitionConfig config) {
    if (time < config.getTimePartitionLowerBoundWithoutOverflow()) {
      return Long.MIN_VALUE;
    }
    if (config.isOriginMayCauseOverflow()) {
      return BigInteger.valueOf(
              getTimePartitionIdWithoutOverflow(
                  time, config.getTimePartitionOrigin(), config.getTimePartitionInterval()))
          .multiply(config.getBigTimePartitionInterval())
          .add(config.getBigTimePartitionOrigin())
          .longValue();
    } else {
      return getTimePartitionId(
                  time, config.getTimePartitionOrigin(), config.getTimePartitionInterval())
              * config.getTimePartitionInterval()
          + config.getTimePartitionOrigin();
    }
  }

  private static long getTimePartitionUpperBoundInternal(
      long time, DatabaseTimePartitionConfig config) {
    if (time >= config.getTimePartitionUpperBoundWithoutOverflow()) {
      return Long.MAX_VALUE;
    }
    long lowerBound = getTimePartitionLowerBoundInternal(time, config);
    return lowerBound == Long.MIN_VALUE
        ? config.getTimePartitionLowerBoundWithoutOverflow()
        : lowerBound + config.getTimePartitionInterval();
  }

  private static long getTimePartitionId(long time, long origin, long interval) {
    time -= origin;
    return time > 0 || time % interval == 0 ? time / interval : time / interval - 1;
  }

  private static long getTimePartitionIdWithoutOverflow(long time, long origin, long interval) {
    BigInteger bigTime = BigInteger.valueOf(time).subtract(BigInteger.valueOf(origin));
    BigInteger bigInterval = BigInteger.valueOf(interval);
    BigInteger partitionId =
        bigTime.compareTo(BigInteger.ZERO) > 0
                || bigTime.remainder(bigInterval).equals(BigInteger.ZERO)
            ? bigTime.divide(bigInterval)
            : bigTime.divide(bigInterval).subtract(BigInteger.ONE);
    return partitionId.longValue();
  }

  // Original global methods for backward compatibility
  @Deprecated
  public static long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  @Deprecated
  public static long getTimePartitionOrigin() {
    return timePartitionOrigin;
  }

  @Deprecated
  public static void setTimePartitionInterval(long timePartitionInterval) {
    TimePartitionUtils.timePartitionInterval = timePartitionInterval;
  }

  // Backward compatibility methods that use global configuration
  @Deprecated
  public static TTimePartitionSlot getTimePartitionSlot(long time) {
    return getTimePartitionSlot(time, null);
  }

  @Deprecated
  public static long getTimePartitionLowerBound(long time) {
    return getTimePartitionLowerBound(time, null);
  }

  @Deprecated
  public static long getTimePartitionUpperBound(long time) {
    return getTimePartitionUpperBound(time, null);
  }

  @Deprecated
  public static long getTimePartitionId(long time) {
    return getTimePartitionId(time, null);
  }

  @Deprecated
  public static long getStartTimeByPartitionId(long partitionId) {
    return getStartTimeByPartitionId(partitionId, null);
  }

  @Deprecated
  public static boolean satisfyPartitionId(long startTime, long endTime, long partitionId) {
    return satisfyPartitionId(startTime, endTime, partitionId, null);
  }

  @Deprecated
  public static boolean satisfyPartitionStartTime(Filter timeFilter, long partitionStartTime) {
    return satisfyPartitionStartTime(timeFilter, partitionStartTime, null);
  }

  @Deprecated
  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId) {
    return satisfyTimePartition(timeFilter, partitionId, null);
  }

  @Deprecated
  public static long getEstimateTimePartitionSize(long startTime, long endTime) {
    return getEstimateTimePartitionSize(startTime, endTime, null);
  }
}
