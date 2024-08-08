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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.Arrays;

public class MemoryEstimationHelper {

  private static final long PARTIAL_PATH_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PartialPath.class);

  private static final long ALIGNED_PATH_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedPath.class);

  private static final long MEASUREMENT_PATH_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedPath.class);

  private MemoryEstimationHelper() {
    // hide the constructor
  }

  /* Responsible for null check. */
  public static long getEstimatedSizeOfAccountableObject(@Nullable final Accountable accountable) {
    return accountable == null ? 0 : accountable.ramBytesUsed();
  }

  public static long getEstimatedSizeOfPartialPath(@Nullable final PartialPath partialPath) {
    if (partialPath == null) {
      return 0;
    }
    long totalSize = 0;
    String[] nodes = partialPath.getNodes();
    if (nodes != null && nodes.length > 0) {
      totalSize += Arrays.stream(nodes).mapToLong(RamUsageEstimator::sizeOf).sum();
    }
    // String member of Path
    if (partialPath instanceof AlignedPath) {
      totalSize += partialPath.getIDeviceID().ramBytesUsed();
      totalSize += RamUsageEstimator.sizeOf(partialPath.getFullPath());

      totalSize += ALIGNED_PATH_INSTANCE_SIZE;
      AlignedPath alignedPath = (AlignedPath) partialPath;
      totalSize +=
          alignedPath.getMeasurementList().stream().mapToLong(RamUsageEstimator::sizeOf).sum();
      totalSize +=
          alignedPath.getSchemaList().stream()
              .mapToLong(schema -> RamUsageEstimator.sizeOf(schema.getMeasurementId()))
              .sum();
    } else if (partialPath instanceof MeasurementPath) {
      totalSize += partialPath.getIDeviceID().ramBytesUsed();
      totalSize += RamUsageEstimator.sizeOf(partialPath.getFullPath());

      totalSize += MEASUREMENT_PATH_INSTANCE_SIZE;
      MeasurementPath measurementPath = (MeasurementPath) partialPath;
      totalSize += RamUsageEstimator.sizeOf(measurementPath.getMeasurementAlias());
      if (measurementPath.getMeasurementSchema() != null) {
        totalSize +=
            RamUsageEstimator.sizeOf(measurementPath.getMeasurementSchema().getMeasurementId());
      }
    } else {
      // the whole path is a device
      totalSize += PARTIAL_PATH_INSTANCE_SIZE;
      totalSize += partialPath.getIDeviceID().ramBytesUsed();
      totalSize += RamUsageEstimator.sizeOf(partialPath.getFullPath());
    }
    return totalSize;
  }
}
