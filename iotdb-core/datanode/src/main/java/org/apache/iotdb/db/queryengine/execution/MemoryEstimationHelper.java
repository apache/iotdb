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

package org.apache.iotdb.db.queryengine.execution;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;

public class MemoryEstimationHelper {
  private MemoryEstimationHelper() {
    // hide the constructor
  }

  /* Responsible for null check. */
  public static long getEstimatedSizeOfMemoryMeasurableObject(MemoryMeasurable memoryMeasurable) {
    return memoryMeasurable == null ? 0 : memoryMeasurable.getEstimatedMemoryUsageInBytes();
  }

  public static long getEstimatedSizeOfAlignedPathWithoutClassSize(AlignedPath partialPath) {
    if (partialPath == null) {
      return 0;
    }
    long totalSize = getEstimatedSizeOfPartialPathWithoutClassSize(partialPath);
    totalSize +=
        partialPath.getMeasurementList().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    totalSize +=
        partialPath.getSchemaList().stream()
            .mapToLong(schema -> RamUsageEstimator.sizeOf(schema.getMeasurementId()))
            .sum();
    return totalSize;
  }

  public static long getEstimatedSizeOfPartialPathWithoutClassSize(PartialPath partialPath) {
    if (partialPath == null) {
      return 0;
    }
    long totalSize = 0;
    String[] nodes = partialPath.getNodes();
    if (nodes != null && nodes.length > 0) {
      totalSize += Arrays.stream(nodes).mapToLong(RamUsageEstimator::sizeOf).sum();
    }
    // String member of Path
    totalSize += RamUsageEstimator.sizeOf(partialPath.getMeasurement());
    totalSize += RamUsageEstimator.sizeOf(partialPath.getDevice());
    totalSize += RamUsageEstimator.sizeOf(partialPath.getFullPath());
    return totalSize;
  }
}
