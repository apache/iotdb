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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

/** It contains all tables' latest column schema */
public class DataNodeTableCache implements ITableCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeTableCache.class);

  private final AtomicLong version = new AtomicLong(0);

  // The database is without "root"
  private final Map<String, Map<String, TsTable>> databaseTableMap = new ConcurrentHashMap<>();

  // The database is without "root"
  private final Map<String, Map<String, Pair<TsTable, Long>>> preUpdateTableMap =
      new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Semaphore fetchTableSemaphore =
      new Semaphore(
          IoTDBDescriptor.getInstance().getConfig().getDataNodeTableCacheSemaphorePermitNum());

  private DataNodeTableCache() {
    // Do nothing
  }

  private static final class DataNodeTableCacheHolder {
    private static final DataNodeTableCache INSTANCE = new DataNodeTableCache();

    private DataNodeTableCacheHolder() {}
  }

  public static DataNodeTableCache getInstance() {
    return DataNodeTableCacheHolder.INSTANCE;
  }

  @Override
  public void init(final byte[] tableInitializationBytes) {
    readWriteLock.writeLock().lock();
    try {
      if (tableInitializationBytes == null) {
        return;
      }
      final Pair<Map<String, List<TsTable>>, Map<String, List<TsTable>>> tableInfo =
          TsTableInternalRPCUtil.deserializeTableInitializationInfo(tableInitializationBytes);
      final Map<String, List<TsTable>> usingMap = tableInfo.left;
      final Map<String, List<TsTable>> preCreateMap = tableInfo.right;
      usingMap.forEach(
          (key, value) ->
              databaseTableMap.put(
                  PathUtils.unQualifyDatabaseName(key),
                  value.stream()
                      .collect(
                          Collectors.toMap(
                              TsTable::getTableName,
                              Function.identity(),
                              (v1, v2) -> v2,
                              ConcurrentHashMap::new))));
      preCreateMap.forEach(
          (key, value) ->
              preUpdateTableMap.put(
                  PathUtils.unQualifyDatabaseName(key),
                  value.stream()
                      .collect(
                          Collectors.toMap(
                              TsTable::getTableName,
                              table -> new Pair<>(table, 0L),
                              (v1, v2) -> v2,
                              ConcurrentHashMap::new))));
      LOGGER.info("Init DataNodeTableCache successfully");
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void preUpdateTable(String database, final TsTable table) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      preUpdateTableMap
          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
          .compute(
              table.getTableName(),
              (k, v) -> {
                if (Objects.isNull(v)) {
                  return new Pair<>(table, 0L);
                } else {
                  v.setLeft(table);
                  v.setRight(v.getRight() + 1);
                  return v;
                }
              });
      LOGGER.info("Pre-update table {}.{} successfully", database, table);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void rollbackUpdateTable(String database, final String tableName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      removeTableFromPreUpdateMap(database, tableName);
      LOGGER.info("Rollback-update table {}.{} successfully", database, tableName);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void removeTableFromPreUpdateMap(final String database, final String tableName) {
    preUpdateTableMap.compute(
        database,
        (k, v) -> {
          if (v == null) {
            throw new IllegalStateException();
          }
          v.get(tableName).setLeft(null);
          return v;
        });
  }

  @Override
  public void commitUpdateTable(String database, final String tableName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      databaseTableMap
          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
          .put(tableName, preUpdateTableMap.get(database).get(tableName).getLeft());
      removeTableFromPreUpdateMap(database, tableName);
      version.incrementAndGet();
      LOGGER.info("Commit-update table {}.{} successfully", database, tableName);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void invalid(String database) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      databaseTableMap.remove(database);
      preUpdateTableMap.remove(database);
      version.incrementAndGet();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @GuardedBy("TableDeviceSchemaCache#writeLock")
  @Override
  public void invalid(String database, final String tableName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      if (databaseTableMap.containsKey(database)) {
        databaseTableMap.get(database).remove(tableName);
      }
      if (preUpdateTableMap.containsKey(database)) {
        preUpdateTableMap.get(database).remove(tableName);
      }
      version.incrementAndGet();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @GuardedBy("TableDeviceSchemaCache#writeLock")
  @Override
  public void invalid(String database, final String tableName, final String columnName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      if (databaseTableMap.containsKey(database)
          && databaseTableMap.get(database).containsKey(tableName)) {
        databaseTableMap.get(database).get(tableName).removeColumnSchema(columnName);
      }
      if (preUpdateTableMap.containsKey(database)
          && preUpdateTableMap.get(database).containsKey(tableName)) {
        final Pair<TsTable, Long> tableVersionPair = preUpdateTableMap.get(database).get(tableName);
        if (Objects.nonNull(tableVersionPair.getLeft())) {
          tableVersionPair.getLeft().removeColumnSchema(columnName);
        }
        tableVersionPair.setRight(tableVersionPair.getRight() + 1);
      }
      version.incrementAndGet();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public long getVersion() {
    return version.get();
  }

  public TsTable getTableInWrite(final String database, final String tableName) {
    final TsTable result = getTableInCache(database, tableName);
    return Objects.nonNull(result) ? result : getTable(database, tableName);
  }

  /**
   * The following logic can handle the cases when configNode failed to clear some table in {@link
   * #preUpdateTableMap}, due to the failure of "commit" or rollback of "pre-update".
   */
  public TsTable getTable(String database, final String tableName) {
    database = PathUtils.unQualifyDatabaseName(database);
    final Map<String, Map<String, Long>> preUpdateTables =
        mayGetTableInPreUpdateMap(database, tableName);
    if (Objects.nonNull(preUpdateTables) && !preUpdateTables.isEmpty()) {
      updateTable(getTablesInConfigNode(preUpdateTables), preUpdateTables);
    }
    return getTableInCache(database, tableName);
  }

  private Map<String, Map<String, Long>> mayGetTableInPreUpdateMap(
      final String database, final String tableName) {
    readWriteLock.readLock().lock();
    try {
      return preUpdateTableMap.containsKey(database)
              && preUpdateTableMap.get(database).containsKey(tableName)
              && Objects.nonNull(preUpdateTableMap.get(database).get(tableName).getLeft())
          ? preUpdateTableMap.entrySet().stream()
              .filter(
                  entry -> {
                    entry
                        .getValue()
                        .entrySet()
                        .removeIf(tableEntry -> Objects.isNull(tableEntry.getValue().getLeft()));
                    return !entry.getValue().isEmpty();
                  })
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry ->
                          entry.getValue().entrySet().stream()
                              .collect(
                                  Collectors.toMap(
                                      Map.Entry::getKey,
                                      innerEntry -> innerEntry.getValue().getRight()))))
          : null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  private Map<String, Map<String, TsTable>> getTablesInConfigNode(
      final Map<String, Map<String, Long>> tableInput) {
    Map<String, Map<String, TsTable>> result = Collections.emptyMap();
    try {
      fetchTableSemaphore.acquire();
      final TFetchTableResp resp =
          ClusterConfigTaskExecutor.getInstance()
              .fetchTables(
                  tableInput.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              entry -> PathUtils.qualifyDatabaseName(entry.getKey()),
                              entry -> entry.getValue().keySet())));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
        result = TsTableInternalRPCUtil.deserializeTsTableFetchResult(resp.getTableInfoMap());
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Interrupted when trying to acquire semaphore when trying to get tables from configNode, ignore.");
    } catch (final Exception e) {
      fetchTableSemaphore.release();
      throw e;
    }
    fetchTableSemaphore.release();
    return result;
  }

  private void updateTable(
      final Map<String, Map<String, TsTable>> fetchedTables,
      final Map<String, Map<String, Long>> previousVersions) {
    readWriteLock.writeLock().lock();
    try {
      final AtomicBoolean isUpdated = new AtomicBoolean(false);
      fetchedTables.forEach(
          (qualifiedDatabase, tableInfoMap) -> {
            final String database = PathUtils.unQualifyDatabaseName(qualifiedDatabase);
            if (preUpdateTableMap.containsKey(database)) {
              tableInfoMap.forEach(
                  (tableName, tsTable) -> {
                    final Pair<TsTable, Long> existingPair =
                        preUpdateTableMap.get(database).get(tableName);
                    if (Objects.isNull(existingPair)
                        || Objects.isNull(existingPair.getLeft())
                        || !Objects.equals(
                            existingPair.getRight(),
                            previousVersions.get(database).get(tableName))) {
                      return;
                    }
                    isUpdated.set(true);
                    LOGGER.info(
                        "Update table {}.{} by table fetch, table in preUpdateMap: {}, new table: {}",
                        database,
                        existingPair.getLeft().getTableName(),
                        existingPair.getLeft(),
                        tsTable);
                    existingPair.setLeft(null);
                    if (Objects.nonNull(tsTable)) {
                      databaseTableMap
                          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
                          .put(tableName, tsTable);
                    } else if (databaseTableMap.containsKey(database)) {
                      databaseTableMap.get(database).remove(tableName);
                    }
                  });
            }
          });
      if (isUpdated.get()) {
        version.incrementAndGet();
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private TsTable getTableInCache(final String database, final String tableName) {
    readWriteLock.readLock().lock();
    try {
      return databaseTableMap.containsKey(database)
          ? databaseTableMap.get(database).get(tableName)
          : null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public boolean isDatabaseExist(final String database) {
    readWriteLock.readLock().lock();
    try {
      return databaseTableMap.containsKey(database);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // Database shall not start with "root"
  public String tryGetInternColumnName(
      final @Nonnull String database,
      final @Nonnull String tableName,
      final @Nonnull String columnName) {
    if (columnName.isEmpty()) {
      return columnName;
    }
    try {
      return databaseTableMap
          .get(database)
          .get(tableName)
          .getColumnSchema(columnName)
          .getColumnName();
    } catch (final Exception e) {
      return null;
    }
  }

  /** Check whether the given path overlap with some table existence. */
  public Pair<String, String> checkTableCreateAndPreCreateOnGivenPath(final PartialPath path) {
    readWriteLock.writeLock().lock();
    try {
      final String pathString = path.getFullPath();
      Pair<String, String> result = checkTableExistenceOnGivenPath(pathString, databaseTableMap);
      if (result == null) {
        result = checkTableExistenceOnGivenPath(pathString, preUpdateTableMap);
      }
      return result;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private Pair<String, String> checkTableExistenceOnGivenPath(
      final String path, final Map<String, ? extends Map<String, ?>> tableMap) {
    final int dbStartIndex = PATH_ROOT.length() + 1;
    for (final Map.Entry<String, ? extends Map<String, ?>> dbEntry : tableMap.entrySet()) {
      final String database = dbEntry.getKey();
      if (!(path.startsWith(database, dbStartIndex)
          && path.length() > dbStartIndex + database.length()
          && path.charAt(dbStartIndex + database.length()) == PATH_SEPARATOR)) {
        continue;
      }
      final int tableStartIndex = dbStartIndex + database.length() + 1;
      for (final String tableName : dbEntry.getValue().keySet()) {
        if (path.startsWith(tableName, tableStartIndex)
            && path.length() > tableStartIndex + tableName.length()
            && path.charAt(tableStartIndex + tableName.length()) == PATH_SEPARATOR) {
          return new Pair<>(database, tableName);
        }
      }
    }
    return null;
  }
}
