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

package org.apache.iotdb.db.schemaengine.lease;

import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;

import com.google.common.util.concurrent.MoreExecutors;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public final class MetadataLeaseTestUtils {

  public static final long T_FENCE_MS = 20_000L;

  private MetadataLeaseTestUtils() {}

  public static MetadataLeaseManager newManager(final AtomicLong nowNanos) {
    return newManager(nowNanos::get);
  }

  public static MetadataLeaseManager newManager(final LongSupplier nowNanos) {
    return newManager(nowNanos, () -> {}, () -> {});
  }

  public static boolean isFenced(final MetadataLeaseManager manager) {
    manager.checkLeaseStatus();
    return manager.isFenced();
  }

  public static void failIfMetadataLeaseFenced(final MetadataLeaseManager manager) {
    manager.checkLeaseStatus();
    if (manager.isFenced()) {
      throw new MetadataLeaseFencedException(
          "Metadata lease is fenced. The local metadata cache is unavailable.");
    }
  }

  static MetadataLeaseManager newManager(
      final AtomicLong nowNanos,
      final MetadataLeaseManager.MetadataAction clearAction,
      final MetadataLeaseManager.MetadataAction pullAction) {
    return newManager(nowNanos::get, clearAction, pullAction);
  }

  static MetadataLeaseManager newManager(
      final LongSupplier nowNanos,
      final MetadataLeaseManager.MetadataAction clearAction,
      final MetadataLeaseManager.MetadataAction pullAction) {
    final MetadataLeaseManager manager =
        new MetadataLeaseManager(
            nowNanos,
            Collections.singletonList(clearAction),
            Collections.singletonList(pullAction),
            MoreExecutors.newDirectExecutorService(),
            500L,
            null);
    manager.updateFenceThresholdMs(T_FENCE_MS);
    return manager;
  }
}
