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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsFileEpochProgressIndexKeeper {

  // data region id -> pipeName -> tsFile path -> max progress index
  private final Map<String, Map<String, Map<String, TsFileResource>>> progressIndexKeeper =
      new ConcurrentHashMap<>();

  public synchronized void registerProgressIndex(
      final String dataRegionId, final String pipeName, final TsFileResource resource) {
    progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
        .putIfAbsent(resource.getTsFilePath(), resource);
  }

  public synchronized void eliminateProgressIndex(
      final String dataRegionId, final @Nonnull String pipeName, final String filePath) {
    progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
        .remove(filePath);
  }

  public synchronized boolean isProgressIndexAfterOrEquals(
      final String dataRegionId,
      final String pipeName,
      final String tsFilePath,
      final ProgressIndex progressIndex) {
    return progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
        .entrySet()
        .stream()
        .filter(entry -> !Objects.equals(entry.getKey(), tsFilePath))
        .map(Entry::getValue)
        .filter(Objects::nonNull)
        .anyMatch(resource -> !resource.getMaxProgressIndex().isAfter(progressIndex));
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTimePartitionProgressIndexKeeperHolder {

    private static final PipeTsFileEpochProgressIndexKeeper INSTANCE =
        new PipeTsFileEpochProgressIndexKeeper();

    private PipeTimePartitionProgressIndexKeeperHolder() {
      // empty constructor
    }
  }

  public static PipeTsFileEpochProgressIndexKeeper getInstance() {
    return PipeTsFileEpochProgressIndexKeeper.PipeTimePartitionProgressIndexKeeperHolder.INSTANCE;
  }

  private PipeTsFileEpochProgressIndexKeeper() {
    // empty constructor
  }
}
