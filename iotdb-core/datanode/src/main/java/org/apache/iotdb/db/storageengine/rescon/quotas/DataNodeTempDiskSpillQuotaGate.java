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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.calc.utils.sort.TempDiskSpillQuotaGate;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceType;
import org.apache.iotdb.rpc.TSStatusCode;

/**
 * DataNode implementation of the spill TEMP_DISK quota hook: charges actually-spilled bytes against
 * the user's read-side TEMP_DISK quota ({@code read_temp_disk_*}) in {@link
 * UserResourceQuotaManager}. Query spill (external sort) is a read-path cost.
 */
public class DataNodeTempDiskSpillQuotaGate implements TempDiskSpillQuotaGate {

  private final UserResourceQuotaManager manager;

  public DataNodeTempDiskSpillQuotaGate(UserResourceQuotaManager manager) {
    this.manager = manager;
  }

  @Override
  public void acquire(long userId, long bytes) throws IoTDBException {
    try {
      manager.acquireOrThrow(
          userId,
          OperationType.READ,
          ResourceType.TEMP_DISK,
          bytes,
          new AcquireContext().setStatementType("SPILL"),
          AcquirePolicy.defaults());
    } catch (UserResourceQuotaExceededException e) {
      throw new IoTDBException(
          e.getMessage(), TSStatusCode.QUOTA_TEMP_DISK_QUERY_NOT_ENOUGH.getStatusCode());
    }
  }

  @Override
  public void release(long userId, long bytes) {
    manager.releaseAmount(userId, OperationType.READ, ResourceType.TEMP_DISK, bytes);
  }
}
