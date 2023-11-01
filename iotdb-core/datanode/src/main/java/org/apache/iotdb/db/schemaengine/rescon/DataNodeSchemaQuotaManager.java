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
  private boolean seriesLimit = false;
  private boolean deviceLimit = false;
  private final AtomicLong seriesRemain = new AtomicLong(0);
  private final AtomicLong deviceRemain = new AtomicLong(0);

  /**
   * Update remain quota.
   *
   * @param seriesRemain -1 means no limit, otherwise it is the remain series quota
   * @param deviceRemain -1 means no limit, otherwise it is the remain device quota
   */
  public void updateRemain(long seriesRemain, long deviceRemain) {
    if (seriesRemain == -1) {
      this.seriesLimit = false;
    } else {
      this.seriesLimit = true;
      this.seriesRemain.set(seriesRemain);
    }
    if (deviceRemain == -1) {
      this.deviceLimit = false;
    } else {
      this.deviceLimit = true;
      this.deviceRemain.set(deviceRemain);
    }
  }

  private void checkMeasurementLevel(long acquireNumber) throws SchemaQuotaExceededException {
    if (seriesLimit) {
      if (seriesRemain.get() <= 0) {
        throw new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.TIMESERIES);
      } else {
        seriesRemain.addAndGet(-acquireNumber);
      }
    }
  }

  private void checkDeviceLevel() throws SchemaQuotaExceededException {
    if (deviceLimit) {
      if (deviceRemain.get() <= 0) {
        throw new SchemaQuotaExceededException(ClusterSchemaQuotaLevel.DEVICE);
      } else {
        deviceRemain.addAndGet(-1L);
      }
    }
  }

  public void check(long acquireSeriesNumber, int acquireDeviceNumber)
      throws SchemaQuotaExceededException {
    if (acquireDeviceNumber > 0) {
      checkDeviceLevel();
    }
    // if pass device check, check measurement level
    try {
      checkMeasurementLevel(acquireSeriesNumber);
    } catch (SchemaQuotaExceededException e) {
      // if measurement level check failed, roll back device remain
      if (acquireDeviceNumber > 0) {
        deviceRemain.addAndGet(1L);
      }
      throw e;
    }
  }

  public boolean isSeriesLimit() {
    return seriesLimit;
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
