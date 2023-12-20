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

package org.apache.iotdb.db.queryengine.load.memory;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.load.memory.block.LoadMemoryBlock;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeviceToTimeseriesSchemasMap {
  private final LoadTsFileMemoryManager loadTsFileMemoryManager =
      LoadTsFileMemoryManager.getInstance();

  private final LoadMemoryBlock block;

  private long totalMemorySizeInBytes =
      IoTDBDescriptor.getInstance().getConfig().getInitLoadMemoryTotalSizeInBytes() / 10;
  private long usedMemorySizeInBytes = 0;

  private final Map<String, Set<MeasurementSchema>> currentBatchDevice2TimeseriesSchemas =
      new HashMap<>();

  public DeviceToTimeseriesSchemasMap() {
    block = loadTsFileMemoryManager.forceAllocate(totalMemorySizeInBytes);
  }

  // false if the memory is still enough and the schema is added successfully, true when it is out
  // of memory.
  public boolean add(String device, MeasurementSchema measurementSchema) {
    boolean isNewDevice = !currentBatchDevice2TimeseriesSchemas.containsKey(device);

    currentBatchDevice2TimeseriesSchemas
        .computeIfAbsent(device, k -> new HashSet<>())
        .add(measurementSchema);

    if (isNewDevice) {
      // estimate the memory size of a new hashmap node and a hash set
      usedMemorySizeInBytes += (100);
    }
    usedMemorySizeInBytes += measurementSchema.serializedSize();

    return usedMemorySizeInBytes > totalMemorySizeInBytes;
  }

  public void clear() {
    currentBatchDevice2TimeseriesSchemas.clear();
    usedMemorySizeInBytes = 0;
  }

  public boolean isEmpty() {
    return currentBatchDevice2TimeseriesSchemas.isEmpty();
  }

  public Set<Map.Entry<String, Set<MeasurementSchema>>> entrySet() {
    return currentBatchDevice2TimeseriesSchemas.entrySet();
  }

  public Set<String> keySet() {
    return currentBatchDevice2TimeseriesSchemas.keySet();
  }

  public void close() {
    currentBatchDevice2TimeseriesSchemas.clear();
    totalMemorySizeInBytes = 0;
    usedMemorySizeInBytes = 0;
    loadTsFileMemoryManager.release(block);
  }
}
