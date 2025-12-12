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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Helper class for estimating memory usage of AST nodes. This class provides utility methods that
 * can be used by Node subclasses to calculate their memory footprint.
 */
public final class AstMemoryEstimationHelper {

  public static final long OPTIONAL_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Optional.class);

  public static final long NODE_LOCATION_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NodeLocation.class);

  private AstMemoryEstimationHelper() {
    // hide the constructor
  }

  public static long getEstimatedSizeOfAccountableObject(@Nullable final Accountable accountable) {
    return accountable == null ? 0 : accountable.ramBytesUsed();
  }

  public static long getEstimatedSizeOfString(@Nullable final String str) {
    return str == null ? 0L : RamUsageEstimator.sizeOf(str);
  }

  public static long getEstimatedSizeOfByteArray(@Nullable final byte[] bytes) {
    return bytes == null ? 0L : RamUsageEstimator.sizeOf(bytes);
  }

  public static long getShallowSizeOfList(@Nullable final List<?> list) {
    return list == null ? 0L : RamUsageEstimator.shallowSizeOf(list);
  }

  public static long getEstimatedSizeOfNodeLocation(@Nullable final NodeLocation location) {
    if (location != null) {
      return OPTIONAL_INSTANCE_SIZE + NODE_LOCATION_INSTANCE_SIZE;
    }
    return 0L;
  }

  public static long getEstimatedSizeOfNodeList(@Nullable final List<? extends Node> children) {
    if (children == null || children.isEmpty()) {
      return 0L;
    }
    long size = RamUsageEstimator.shallowSizeOf(children);
    for (Node child : children) {
      if (child != null) {
        size += child.ramBytesUsed();
      }
    }
    return size;
  }

  public static long getEstimatedSizeOfStringList(@Nullable final List<String> strings) {
    if (strings == null || strings.isEmpty()) {
      return 0L;
    }
    long size = RamUsageEstimator.shallowSizeOf(strings);
    for (String str : strings) {
      if (str != null) {
        size += RamUsageEstimator.sizeOf(str);
      }
    }
    return size;
  }

  public static long getEstimatedSizeOfIntegerList(@Nullable final List<Integer> integers) {
    if (integers == null || integers.isEmpty()) {
      return 0L;
    }
    long size = RamUsageEstimator.shallowSizeOf(integers);
    // Integer objects are typically cached by JVM for small values, but we estimate
    // the overhead for Integer objects (16 bytes each)
    for (Integer integer : integers) {
      if (integer != null) {
        size += RamUsageEstimator.shallowSizeOfInstance(Integer.class);
      }
    }
    return size;
  }
}
