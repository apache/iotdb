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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.matcher;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class CachedSchemaPatternMatcher implements PipeDataRegionMatcher {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CachedSchemaPatternMatcher.class);

  protected static final String TREE_MODEL_EVENT_TABLE_NAME_PREFIX = PATH_ROOT + PATH_SEPARATOR;

  protected final ReentrantReadWriteLock lock;
  protected final Set<PipeRealtimeDataRegionSource> sources;

  protected final Cache<IDeviceID, Set<PipeRealtimeDataRegionSource>> deviceToSourcesCache;
  protected final Cache<Pair<String, IDeviceID>, Set<PipeRealtimeDataRegionSource>>
      databaseAndTableToSourcesCache;

  public CachedSchemaPatternMatcher() {
    this.lock = new ReentrantReadWriteLock();
    // Should be thread-safe because the sources will be returned by {@link #match} and
    // iterated by {@link #assignToSource}, at the same time the sources may be added or
    // removed by {@link #register} and {@link #deregister}.
    this.sources = new CopyOnWriteArraySet<>();
    this.deviceToSourcesCache =
        Caffeine.newBuilder()
            .maximumSize(PipeConfig.getInstance().getPipeSourceMatcherCacheSize())
            .build();
    this.databaseAndTableToSourcesCache =
        Caffeine.newBuilder()
            .maximumSize(PipeConfig.getInstance().getPipeSourceMatcherCacheSize())
            .build();
  }

  @Override
  public void register(final PipeRealtimeDataRegionSource source) {
    lock.writeLock().lock();
    try {
      sources.add(source);
      deviceToSourcesCache.invalidateAll();
      databaseAndTableToSourcesCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(final PipeRealtimeDataRegionSource source) {
    lock.writeLock().lock();
    try {
      sources.remove(source);
      deviceToSourcesCache.invalidateAll();
      databaseAndTableToSourcesCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void invalidateCache() {
    lock.writeLock().lock();
    try {
      // Will invalidate device cache
      databaseAndTableToSourcesCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getRegisterCount() {
    lock.readLock().lock();
    try {
      return sources.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Pair<Set<PipeRealtimeDataRegionSource>, Set<PipeRealtimeDataRegionSource>> match(
      final PipeRealtimeEvent event) {
    final Set<PipeRealtimeDataRegionSource> matchedSources = new HashSet<>();

    lock.readLock().lock();
    try {
      if (sources.isEmpty()) {
        return new Pair<>(matchedSources, sources);
      }

      // HeartbeatEvent will be assigned to all sources
      if (event.getEvent() instanceof PipeHeartbeatEvent) {
        return new Pair<>(sources, Collections.EMPTY_SET);
      }

      // TODO: consider table pattern?
      // Deletion event will be assigned to sources listened to it
      if (event.getEvent() instanceof PipeDeleteDataNodeEvent) {
        sources.stream()
            .filter(PipeRealtimeDataRegionSource::shouldExtractDeletion)
            .forEach(matchedSources::add);
        return new Pair<>(matchedSources, findUnmatchedSources(matchedSources));
      }

      final Set<String> tableNames = new HashSet<>();
      for (final Map.Entry<IDeviceID, String[]> entry : event.getSchemaInfo().entrySet()) {
        final IDeviceID deviceID = entry.getKey();

        // TODO: Check the role to determine whether to match with tree model or table model
        if (deviceID instanceof PlainDeviceID
            || deviceID.getTableName().startsWith(TREE_MODEL_EVENT_TABLE_NAME_PREFIX)
            || deviceID.getTableName().equals(PATH_ROOT)) {
          matchTreeModelEvent(deviceID, entry.getValue(), matchedSources);
        } else {
          tableNames.add(deviceID.getTableName());
          matchTableModelEvent(
              event.getEvent() instanceof PipeInsertionEvent
                  ? ((PipeInsertionEvent) event.getEvent()).getTableModelDatabaseName()
                  : null,
              deviceID,
              matchedSources);
        }

        if (matchedSources.size() == sources.size()) {
          break;
        }
      }

      if (event.getEvent() instanceof PipeTsFileInsertionEvent) {
        final PipeTsFileInsertionEvent tsFileInsertionEvent =
            (PipeTsFileInsertionEvent) event.getEvent();
        if (tsFileInsertionEvent.isTableModelEvent()) {
          tsFileInsertionEvent.setTableNames(tableNames);
        } else {
          tsFileInsertionEvent.setTreeSchemaMap(event.getSchemaInfo());
        }
      }

      return new Pair<>(matchedSources, findUnmatchedSources(matchedSources));
    } finally {
      lock.readLock().unlock();
    }
  }

  private Set<PipeRealtimeDataRegionSource> findUnmatchedSources(
      final Set<PipeRealtimeDataRegionSource> matchedSources) {
    final Set<PipeRealtimeDataRegionSource> unmatchedSources = new HashSet<>();
    for (final PipeRealtimeDataRegionSource source : sources) {
      if (!matchedSources.contains(source)) {
        unmatchedSources.add(source);
      }
    }
    return unmatchedSources;
  }

  protected void matchTreeModelEvent(
      final IDeviceID device,
      final String[] measurements,
      final Set<PipeRealtimeDataRegionSource> matchedSources) {
    // 1. try to get matched sources from cache, if not success, match them by device
    final Set<PipeRealtimeDataRegionSource> sourcesFilteredByDevice =
        deviceToSourcesCache.get(device, this::filterSourcesByDevice);
    // this would not happen
    if (sourcesFilteredByDevice == null) {
      LOGGER.warn(
          "Sources filtered by device is null when matching sources for tree model event.",
          new Exception());
      return;
    }

    // 2. filter matched candidate sources by measurements
    if (measurements.length == 0) {
      // `measurements` is empty (only in case of tsfile event). match all sources.
      //
      // case 1: the pattern can match all measurements of the device.
      // in this case, the source can be matched without checking the measurements.
      //
      // case 2: the pattern may match some measurements of the device.
      // in this case, we can't get all measurements efficiently here,
      // so we just ASSUME the source matches and do more checks later.
      matchedSources.addAll(sourcesFilteredByDevice);
    } else {
      // `measurements` is not empty (only in case of tablet event).
      // Match sources by measurements.
      sourcesFilteredByDevice.forEach(
          source -> {
            if (matchedSources.size() == sources.size()) {
              return;
            }

            final TreePattern pattern = source.getTreePattern();
            if (Objects.isNull(pattern) || pattern.isRoot() || pattern.coversDevice(device)) {
              // The pattern can match all measurements of the device.
              matchedSources.add(source);
            } else {
              for (final String measurement : measurements) {
                // Ignore null measurement for partial insert
                if (measurement == null) {
                  continue;
                }

                if (pattern.matchesMeasurement(device, measurement)) {
                  matchedSources.add(source);
                  // There would be no more matched sources because the measurements are
                  // unique
                  break;
                }
              }
            }
          });
    }
  }

  protected Set<PipeRealtimeDataRegionSource> filterSourcesByDevice(final IDeviceID device) {
    final Set<PipeRealtimeDataRegionSource> filteredSources = new HashSet<>();

    for (final PipeRealtimeDataRegionSource source : sources) {
      // Return if the source only extract deletion
      if (!source.shouldExtractInsertion()) {
        continue;
      }

      final TreePattern treePattern = source.getTreePattern();
      if (Objects.isNull(treePattern)
          || (treePattern.isTreeModelDataAllowedToBeCaptured()
              && treePattern.mayOverlapWithDevice(device))) {
        filteredSources.add(source);
      }
    }

    return filteredSources;
  }

  protected void matchTableModelEvent(
      final String databaseName,
      final IDeviceID tableName,
      final Set<PipeRealtimeDataRegionSource> matchedSources) {
    // this would not happen
    if (databaseName == null) {
      LOGGER.warn(
          "Database name is null when matching sources for table model event.", new Exception());
      return;
    }

    final Set<PipeRealtimeDataRegionSource> sourcesFilteredByDatabaseAndTable =
        databaseAndTableToSourcesCache.get(
            new Pair<>(databaseName, tableName), this::filterSourcesByDatabaseAndTable);
    // this would not happen
    if (sourcesFilteredByDatabaseAndTable == null) {
      LOGGER.warn(
          "Sources filtered by database and table is null when matching sources for table model event.",
          new Exception());
      return;
    }
    matchedSources.addAll(sourcesFilteredByDatabaseAndTable);
  }

  protected Set<PipeRealtimeDataRegionSource> filterSourcesByDatabaseAndTable(
      final Pair<String, IDeviceID> databaseNameAndTableName) {
    final Set<PipeRealtimeDataRegionSource> filteredSources = new HashSet<>();

    for (final PipeRealtimeDataRegionSource source : sources) {
      // Return if the source only extract deletion
      if (!source.shouldExtractInsertion()) {
        continue;
      }

      final TablePattern tablePattern = source.getTablePattern();
      if (matchesTablePattern(tablePattern, databaseNameAndTableName)
          && (!source.isSkipIfNoPrivileges()
              || notFilteredByAccess(
                  new UserEntity(source.getUserId(), source.getUserName(), source.getCliHostname()),
                  databaseNameAndTableName))) {
        filteredSources.add(source);
      }
    }

    return filteredSources;
  }

  private boolean matchesTablePattern(
      final TablePattern tablePattern, final Pair<String, IDeviceID> databaseNameAndTableName) {
    return Objects.isNull(tablePattern)
        || (tablePattern.isTableModelDataAllowedToBeCaptured()
            && tablePattern.matchesDatabase(databaseNameAndTableName.getLeft())
            && tablePattern.matchesTable(databaseNameAndTableName.getRight().getTableName()));
  }

  private boolean notFilteredByAccess(
      final UserEntity userEntity, final Pair<String, IDeviceID> databaseNameAndTableName) {
    return AuthorityChecker.getAccessControl()
        .checkCanSelectFromTable4Pipe(
            userEntity.getUsername(),
            new QualifiedObjectName(
                databaseNameAndTableName.getLeft(),
                databaseNameAndTableName.getRight().getTableName()),
            userEntity);
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      sources.clear();
      deviceToSourcesCache.invalidateAll();
      deviceToSourcesCache.cleanUp();
      databaseAndTableToSourcesCache.invalidateAll();
      databaseAndTableToSourcesCache.cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
