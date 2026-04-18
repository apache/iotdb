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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.utils.Pair;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsFileEpochProgressIndexAndFlushManager {
  private static volatile long lastFlushTime = Long.MIN_VALUE;

  // data region id -> task scope id -> tsFile path -> max progress index
  private final Map<Integer, Map<String, Map<String, Pair<TsFileResource, Long>>>>
      progressIndexKeeper = new ConcurrentHashMap<>();

  public synchronized void registerResource(
      final int dataRegionId, final String taskScopeID, final TsFileResource resource) {
    progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(taskScopeID, k -> new ConcurrentHashMap<>())
        .putIfAbsent(resource.getTsFilePath(), new Pair<>(resource, System.currentTimeMillis()));
  }

  public synchronized void markAsExtracted(
      final int dataRegionId, final String pipeName, final String filePath) {
    final Pair<TsFileResource, Long> pair =
        progressIndexKeeper
            .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
            .get(filePath);
    if (Objects.nonNull(pair)) {
      pair.setRight(Long.MAX_VALUE);
    }
  }

  public void flushAllTimeoutTsFiles() {
    final long intervalMillis =
        PipeConfig.getInstance().getPipeTsFileFlushIntervalSeconds() * 1000L;
    if (System.currentTimeMillis() - intervalMillis < lastFlushTime) {
      return;
    }
    lastFlushTime = System.currentTimeMillis();
    progressIndexKeeper.forEach(
        (regionId, map) ->
            map.values()
                .forEach(
                    fileMap ->
                        fileMap.forEach(
                            (path, pair) -> {
                              if (System.currentTimeMillis() - intervalMillis >= pair.getRight()) {
                                StorageEngine.getInstance()
                                    .getDataRegion(new DataRegionId(regionId))
                                    .asyncCloseOneTsFileProcessor(pair.getLeft());
                                pair.setRight(Long.MAX_VALUE);
                              }
                            })));
  }

  public synchronized void eliminateProgressIndex(
      final int dataRegionId, final @Nonnull String taskScopeID, final String filePath) {
    final Map<String, Map<String, Pair<TsFileResource, Long>>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return;
    }

    final Map<String, Pair<TsFileResource, Long>> tsFileProgressIndexKeeper =
        scopeProgressIndexKeeper.get(taskScopeID);
    if (tsFileProgressIndexKeeper == null) {
      return;
    }

    tsFileProgressIndexKeeper.remove(filePath);
    if (tsFileProgressIndexKeeper.isEmpty()) {
      scopeProgressIndexKeeper.remove(taskScopeID);
      if (scopeProgressIndexKeeper.isEmpty()) {
        progressIndexKeeper.remove(dataRegionId);
      }
    }
  }

  public synchronized void clearProgressIndex(
      final int dataRegionId, final @Nonnull String taskScopeID) {
    final Map<String, Map<String, Pair<TsFileResource, Long>>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return;
    }

    scopeProgressIndexKeeper.remove(taskScopeID);
    if (scopeProgressIndexKeeper.isEmpty()) {
      progressIndexKeeper.remove(dataRegionId);
    }
  }

  public synchronized boolean containsTsFile(
      final int dataRegionId, final @Nonnull String taskScopeID, final String tsFilePath) {
    final Map<String, Map<String, Pair<TsFileResource, Long>>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return false;
    }

    final Map<String, Pair<TsFileResource, Long>> tsFileProgressIndexKeeper =
        scopeProgressIndexKeeper.get(taskScopeID);
    return tsFileProgressIndexKeeper != null && tsFileProgressIndexKeeper.containsKey(tsFilePath);
  }

  public synchronized boolean isProgressIndexAfterOrEquals(
      final int dataRegionId,
      final String taskScopeID,
      final String tsFilePath,
      final ProgressIndex progressIndex) {
    final Map<String, Map<String, Pair<TsFileResource, Long>>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return false;
    }

    final Map<String, Pair<TsFileResource, Long>> tsFileProgressIndexKeeper =
        scopeProgressIndexKeeper.get(taskScopeID);
    if (tsFileProgressIndexKeeper == null) {
      return false;
    }

    return tsFileProgressIndexKeeper.entrySet().stream()
        .filter(entry -> !Objects.equals(entry.getKey(), tsFilePath))
        .map(Entry::getValue)
        .filter(Objects::nonNull)
        .anyMatch(resource -> !resource.getLeft().getMaxProgressIndex().isAfter(progressIndex));
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTimePartitionProgressIndexKeeperHolder {

    private static final PipeTsFileEpochProgressIndexAndFlushManager INSTANCE =
        new PipeTsFileEpochProgressIndexAndFlushManager();

    private PipeTimePartitionProgressIndexKeeperHolder() {
      // empty constructor
    }
  }

  public static PipeTsFileEpochProgressIndexAndFlushManager getInstance() {
    return PipeTsFileEpochProgressIndexAndFlushManager.PipeTimePartitionProgressIndexKeeperHolder
        .INSTANCE;
  }

  private PipeTsFileEpochProgressIndexAndFlushManager() {
    // empty constructor
  }
}
