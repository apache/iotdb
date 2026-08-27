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

package org.apache.iotdb.db.conf;

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public enum TieredStorageMigrationFileSelectionStrategy {
  OLDEST_TIME_PARTITION_FIRST,
  LARGEST_TSFILE_FIRST;

  public static final TieredStorageMigrationFileSelectionStrategy DEFAULT =
      OLDEST_TIME_PARTITION_FIRST;

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TieredStorageMigrationFileSelectionStrategy.class);

  public static TieredStorageMigrationFileSelectionStrategy fromString(String value) {
    if (value == null) {
      return DEFAULT;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          DataNodeMiscMessages
              .LOG_UNKNOWN_TIERED_STORAGE_MIGRATION_FILE_SELECTION_STRATEGY_ARG_USE_DEFAULT_STRATEGY_ARG_C2A73E2D,
          value,
          DEFAULT);
      return DEFAULT;
    }
  }

  public Iterator<TsFileResource> createMigrationCandidateIterator(
      List<TsFileResource> candidates) {
    List<TsFileResource> orderedCandidates = new ArrayList<>(candidates);
    orderedCandidates.sort(this::compare);
    return orderedCandidates.iterator();
  }

  public int compare(TsFileResource left, TsFileResource right) {
    int result = Integer.compare(left.getTierLevel(), right.getTierLevel());
    if (result == 0 && this == LARGEST_TSFILE_FIRST) {
      result = Long.compare(right.getTsFileSize(), left.getTsFileSize());
    }
    return result == 0 ? compareOldestTimePartitionFirst(left, right) : result;
  }

  private static int compareOldestTimePartitionFirst(TsFileResource left, TsFileResource right) {
    int result = Integer.compare(left.getTierLevel(), right.getTierLevel());
    if (result == 0) {
      result = Long.compare(left.getTimePartition(), right.getTimePartition());
    }
    if (result == 0) {
      if (left.isSeq() && !right.isSeq()) {
        result = -1;
      } else if (!left.isSeq() && right.isSeq()) {
        result = 1;
      }
    }
    if (result == 0) {
      result = Long.compare(left.getVersion(), right.getVersion());
    }
    return result;
  }
}
