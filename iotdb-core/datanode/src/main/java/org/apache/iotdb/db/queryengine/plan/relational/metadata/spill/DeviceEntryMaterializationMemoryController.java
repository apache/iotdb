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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;

/** Controls the total in-memory DeviceEntry buffers owned by all Region materializers. */
public final class DeviceEntryMaterializationMemoryController {

  private final long memoryLimitInBytes;
  private final Map<AbstractDeviceEntryMaterializer, Long> retainedBytesByMaterializer =
      new IdentityHashMap<>();
  private long retainedBytes;

  public DeviceEntryMaterializationMemoryController(long memoryLimitInBytes) {
    if (memoryLimitInBytes <= 0) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_DEVICEENTRY_MATERIALIZATION_MEMORY_LIMIT_MUST_BE_POSITIVE_ARG_2EB63404,
              memoryLimitInBytes));
    }
    this.memoryLimitInBytes = memoryLimitInBytes;
  }

  public void append(AbstractDeviceEntryMaterializer materializer, DeviceEntry deviceEntry)
      throws IOException {
    materializer.append(deviceEntry);
    long entryRamBytes = deviceEntry.ramBytesUsed();
    retainedBytesByMaterializer.merge(materializer, entryRamBytes, Long::sum);
    retainedBytes += entryRamBytes;
    enforceMemoryLimit();
  }

  private void enforceMemoryLimit() throws IOException {
    while (retainedBytes > memoryLimitInBytes) {
      AbstractDeviceEntryMaterializer largest = null;
      long largestRetainedBytes = 0;
      for (Map.Entry<AbstractDeviceEntryMaterializer, Long> entry :
          retainedBytesByMaterializer.entrySet()) {
        if (entry.getValue() > largestRetainedBytes) {
          largest = entry.getKey();
          largestRetainedBytes = entry.getValue();
        }
      }
      if (largest == null) {
        throw new IllegalStateException(
            DataNodeQueryMessages
                .EXCEPTION_NO_MATERIALIZER_IS_AVAILABLE_TO_ENFORCE_THE_DEVICEENTRY_MEMORY_LIMIT_FD0604AF);
      }
      largest.forceSpill();
      retainedBytesByMaterializer.put(largest, 0L);
      retainedBytes -= largestRetainedBytes;
    }
  }
}
