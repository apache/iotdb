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

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MemoryEstimationHelper {

  private static final long PARTIAL_PATH_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PartialPath.class);

  private static final long ALIGNED_PATH_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedPath.class);

  private static final long MEASUREMENT_PATH_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedPath.class);

  private static final long ARRAY_LIST_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);
  private static final long INTEGER_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Integer.class);
  public static final long TIME_RANGE_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimeRange.class);

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
              .mapToLong(schema -> RamUsageEstimator.sizeOf(schema.getMeasurementName()))
              .sum();
    } else if (partialPath instanceof MeasurementPath) {
      totalSize += partialPath.getIDeviceID().ramBytesUsed();
      totalSize += RamUsageEstimator.sizeOf(partialPath.getFullPath());

      totalSize += MEASUREMENT_PATH_INSTANCE_SIZE;
      MeasurementPath measurementPath = (MeasurementPath) partialPath;
      totalSize += RamUsageEstimator.sizeOf(measurementPath.getMeasurementAlias());
      if (measurementPath.getMeasurementSchema() != null) {
        totalSize +=
            RamUsageEstimator.sizeOf(measurementPath.getMeasurementSchema().getMeasurementName());
      }
    } else {
      // the whole path is a device
      totalSize += PARTIAL_PATH_INSTANCE_SIZE;
      totalSize += partialPath.getIDeviceID().ramBytesUsed();
      totalSize += RamUsageEstimator.sizeOf(partialPath.getFullPath());
    }
    return totalSize;
  }

  public static long getEstimatedSizeOfMeasurementPathNodes(
      @Nullable final PartialPath partialPath) {
    if (partialPath == null) {
      return 0;
    }
    long totalSize = MEASUREMENT_PATH_INSTANCE_SIZE;
    String[] nodes = partialPath.getNodes();
    if (nodes != null && nodes.length > 0) {
      totalSize += Arrays.stream(nodes).mapToLong(RamUsageEstimator::sizeOf).sum();
    }
    return totalSize;
  }

  // This method should only be called if the content in the current PartialPath comes from other
  // structures whose memory cost have already been calculated.
  public static long getEstimatedSizeOfCopiedPartialPath(@Nullable final PartialPath partialPath) {
    if (partialPath == null) {
      return 0;
    }
    return PARTIAL_PATH_INSTANCE_SIZE + RamUsageEstimator.shallowSizeOf(partialPath.getNodes());
  }

  public static long getEstimatedSizeOfIntegerArrayList(List<Integer> integerArrayList) {
    if (integerArrayList == null) {
      return 0L;
    }
    long size = ARRAY_LIST_INSTANCE_SIZE;
    size +=
        (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + (long) integerArrayList.size() * (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    size += INTEGER_INSTANCE_SIZE * integerArrayList.size();
    return RamUsageEstimator.alignObjectSize(size);
  }
}
