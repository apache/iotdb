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

  // data region id -> task scope id -> tsFile path -> max progress index
  private final Map<Integer, Map<String, Map<String, TsFileResource>>> progressIndexKeeper =
      new ConcurrentHashMap<>();

  public synchronized void registerProgressIndex(
      final int dataRegionId, final String taskScopeID, final TsFileResource resource) {
    progressIndexKeeper
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(taskScopeID, k -> new ConcurrentHashMap<>())
        .putIfAbsent(resource.getTsFilePath(), resource);
  }

  public synchronized void eliminateProgressIndex(
      final int dataRegionId, final @Nonnull String taskScopeID, final String filePath) {
    final Map<String, Map<String, TsFileResource>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return;
    }

    final Map<String, TsFileResource> tsFileProgressIndexKeeper =
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
    final Map<String, Map<String, TsFileResource>> scopeProgressIndexKeeper =
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
    final Map<String, Map<String, TsFileResource>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return false;
    }

    final Map<String, TsFileResource> tsFileProgressIndexKeeper =
        scopeProgressIndexKeeper.get(taskScopeID);
    return tsFileProgressIndexKeeper != null && tsFileProgressIndexKeeper.containsKey(tsFilePath);
  }

  public synchronized boolean isProgressIndexAfterOrEquals(
      final int dataRegionId,
      final String taskScopeID,
      final String tsFilePath,
      final ProgressIndex progressIndex) {
    final Map<String, Map<String, TsFileResource>> scopeProgressIndexKeeper =
        progressIndexKeeper.get(dataRegionId);
    if (scopeProgressIndexKeeper == null) {
      return false;
    }

    final Map<String, TsFileResource> tsFileProgressIndexKeeper =
        scopeProgressIndexKeeper.get(taskScopeID);
    if (tsFileProgressIndexKeeper == null) {
      return false;
    }

    return tsFileProgressIndexKeeper.entrySet().stream()
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
