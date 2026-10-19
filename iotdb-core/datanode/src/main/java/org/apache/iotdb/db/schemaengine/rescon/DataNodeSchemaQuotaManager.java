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

package org.apache.iotdb.db.schemaengine.rescon;

import org.apache.iotdb.commons.schema.ClusterSchemaQuotaLevel;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;

import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("java:S6548") // do not warn about singleton class
public class DataNodeSchemaQuotaManager {
  private volatile boolean measurementLimit = false;
  private volatile boolean deviceLimit = false;
  private final AtomicLong measurementRemain = new AtomicLong(0);
  private final AtomicLong deviceRemain = new AtomicLong(0);
  private long measurementQuota = -1;
  private long clusterMeasurementCount;
  private long localMeasurementCount;

  /**
   * Update remain quota.
   *
   * @param measurementRemain -1 means no limit, otherwise it is the remain measurement quota
   * @param deviceRemain -1 means no limit, otherwise it is the remain device quota
   */
  public synchronized void updateRemain(long measurementRemain, long deviceRemain) {
    measurementQuota = -1;
    clusterMeasurementCount = 0;
    localMeasurementCount = 0;
    if (measurementRemain == -1) {
      this.measurementLimit = false;
    } else {
      this.measurementLimit = true;
      this.measurementRemain.set(measurementRemain);
    }
    if (deviceRemain == -1) {
      this.deviceLimit = false;
    } else {
      this.deviceLimit = true;
      this.deviceRemain.set(deviceRemain);
    }
  }

  /**
   * Cache the latest ConfigNode quota and usage together with the local quota usage. Successful
   * acquisitions are added to the local count until the next heartbeat refreshes it.
   *
   * @param measurementQuota -1 means no limit
   */
  public synchronized void updateQuota(
      long measurementQuota,
      long clusterMeasurementCount,
      long localMeasurementCount,
      long deviceRemain) {
    updateRemain(
        measurementQuota == -1 ? -1 : Math.max(0, measurementQuota - clusterMeasurementCount),
        deviceRemain);
    this.measurementQuota = measurementQuota;
    this.clusterMeasurementCount = clusterMeasurementCount;
    this.localMeasurementCount = localMeasurementCount;
  }

  private void checkMeasurementLevel(long acquireNumber) throws SchemaQuotaExceededException {
    if (measurementLimit && acquireNumber > 0) {
      if (measurementQuota != -1) {
        final long currentCount = Math.max(localMeasurementCount, clusterMeasurementCount);
        // Subtract before comparing to avoid overflowing when adding the requested count.
        if (currentCount > measurementQuota || acquireNumber > measurementQuota - currentCount) {
          throw new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.TIMESERIES);
        }
        localMeasurementCount += acquireNumber;
      } else if (acquireNumber > measurementRemain.get()) {
        throw new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.TIMESERIES);
      } else {
        measurementRemain.addAndGet(-acquireNumber);
      }
    }
  }

  private void checkDeviceLevel(final int acquireNumber) throws SchemaQuotaExceededException {
    if (deviceLimit) {
      if (acquireNumber > deviceRemain.get()) {
        throw new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.DEVICE);
      } else {
        deviceRemain.addAndGet(-acquireNumber);
      }
    }
  }

  public synchronized void check(final long acquireMeasurementNumber, final int acquireDeviceNumber)
      throws SchemaQuotaExceededException {
    if (acquireDeviceNumber > 0) {
      checkDeviceLevel(acquireDeviceNumber);
    }
    // if pass device check, check measurement level
    try {
      checkMeasurementLevel(acquireMeasurementNumber);
    } catch (final SchemaQuotaExceededException e) {
      // if measurement level check failed, roll back device remain
      if (acquireDeviceNumber > 0 && deviceLimit) {
        deviceRemain.addAndGet(acquireDeviceNumber);
      }
      throw e;
    }
  }

  public boolean isMeasurementLimit() {
    return measurementLimit;
  }

  public boolean isDeviceLimit() {
    return deviceLimit;
  }

  private DataNodeSchemaQuotaManager() {}

  public static DataNodeSchemaQuotaManager getInstance() {
    return DataNodeSchemaQuotaManager.DataNodeSchemaQuotaManagerHolder.INSTANCE;
  }

  private static class DataNodeSchemaQuotaManagerHolder {
    private static final DataNodeSchemaQuotaManager INSTANCE = new DataNodeSchemaQuotaManager();

    private DataNodeSchemaQuotaManagerHolder() {
      // empty constructor
    }
  }
}
