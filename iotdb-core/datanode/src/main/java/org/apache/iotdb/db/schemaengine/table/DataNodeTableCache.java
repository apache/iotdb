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

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

/** It contains all tables' latest column schema */
public class DataNodeTableCache implements ITableCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeTableCache.class);

  private final Map<String, Map<String, TsTable>> databaseTableMap = new ConcurrentHashMap<>();

  private final Map<String, Map<String, TsTable>> preUpdateTableMap = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

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
      saveUpdatedTableInfo(usingMap, databaseTableMap);
      saveUpdatedTableInfo(preCreateMap, preUpdateTableMap);
      LOGGER.info("Init DataNodeTableCache successfully");
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void saveUpdatedTableInfo(
      final Map<String, List<TsTable>> tableMap,
      final Map<String, Map<String, TsTable>> localTableMap) {
    tableMap.forEach(
        (key, value) ->
            localTableMap.put(
                key,
                value.stream()
                    .collect(
                        Collectors.toMap(
                            TsTable::getTableName,
                            Function.identity(),
                            (v1, v2) -> v2,
                            ConcurrentHashMap::new))));
  }

  @Override
  public void preUpdateTable(String database, final TsTable table) {
    database = PathUtils.qualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      preUpdateTableMap
          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
          .put(table.getTableName(), table);
      LOGGER.info("Pre-update table {}.{} successfully", database, table);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void rollbackUpdateTable(String database, final String tableName) {
    database = PathUtils.qualifyDatabaseName(database);
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
          v.remove(tableName);
          if (v.isEmpty()) {
            return null;
          } else {
            return v;
          }
        });
  }

  @Override
  public void commitUpdateTable(String database, final String tableName) {
    database = PathUtils.qualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      final TsTable table = preUpdateTableMap.get(database).get(tableName);
      databaseTableMap
          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
          .put(tableName, table);
      removeTableFromPreUpdateMap(database, tableName);
      LOGGER.info("Commit-update table {}.{} successfully", database, tableName);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void invalid(String database) {
    database = PathUtils.qualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      databaseTableMap.remove(database);
      preUpdateTableMap.remove(database);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public TsTable getTable(String database, final String tableName) {
    database = PathUtils.qualifyDatabaseName(database);
    readWriteLock.readLock().lock();
    try {
      if (databaseTableMap.containsKey(database)) {
        return databaseTableMap.get(database).get(tableName);
      }
      return null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public Optional<List<TsTable>> getTables(String database) {
    database = PathUtils.qualifyDatabaseName(database);
    readWriteLock.readLock().lock();
    try {
      final Map<String, TsTable> tableMap = databaseTableMap.get(database);
      return tableMap != null ? Optional.of(new ArrayList<>(tableMap.values())) : Optional.empty();
    } finally {
      readWriteLock.readLock().unlock();
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
      final String path, final Map<String, Map<String, TsTable>> tableMap) {
    final int dbStartIndex = PATH_ROOT.length() + 1;
    for (final Map.Entry<String, Map<String, TsTable>> dbEntry : tableMap.entrySet()) {
      final String database = dbEntry.getKey();
      if (!(path.startsWith(database, dbStartIndex)
          && path.charAt(dbStartIndex + database.length()) == PATH_SEPARATOR)) {
        continue;
      }
      final int tableStartIndex = dbStartIndex + database.length() + 1;
      for (final String tableName : dbEntry.getValue().keySet()) {
        if (path.startsWith(tableName, tableStartIndex)
            && path.charAt(tableStartIndex + tableName.length()) == PATH_SEPARATOR) {
          return new Pair<>(database, tableName);
        }
      }
    }
    return null;
  }
}
