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
package org.apache.iotdb.db.metadata.rescon;

import org.apache.iotdb.commons.schema.ClusterSchemaQuotaLevel;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;

import java.util.concurrent.atomic.AtomicLong;

public class DataNodeSchemaQuotaManager {

  private ClusterSchemaQuotaLevel level =
      ClusterSchemaQuotaLevel.valueOf(
          IoTDBDescriptor.getInstance().getConfig().getClusterSchemaLimitLevel().toUpperCase());
  private long limit =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getClusterMaxSchemaCount(); // -1 means no limitation
  private final AtomicLong remain = new AtomicLong(0);

  public void updateRemain(long totalCount) {
    this.remain.getAndSet(limit - totalCount);
  }

  public void checkMeasurementLevel(int acquireNumber) throws SchemaQuotaExceededException {
    if (limit > 0 && level.equals(ClusterSchemaQuotaLevel.MEASUREMENT)) {
      if (remain.get() <= 0) {
        throw new SchemaQuotaExceededException(level, limit);
      } else {
        remain.addAndGet(-acquireNumber);
      }
    }
  }

  public void checkDeviceLevel() throws SchemaQuotaExceededException {
    if (limit > 0 && level.equals(ClusterSchemaQuotaLevel.DEVICE)) {
      if (remain.get() <= 0) {
        throw new SchemaQuotaExceededException(level, limit);
      } else {
        remain.addAndGet(-1L);
      }
    }
  }

  public void updateConfiguration() {
    this.level =
        ClusterSchemaQuotaLevel.valueOf(
            IoTDBDescriptor.getInstance().getConfig().getClusterSchemaLimitLevel());
    long oldLimit = limit;
    this.limit = IoTDBDescriptor.getInstance().getConfig().getClusterMaxSchemaCount();
    this.remain.addAndGet(limit - oldLimit);
  }

  public ClusterSchemaQuotaLevel getLevel() {
    return level;
  }

  public long getLimit() {
    return limit;
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
