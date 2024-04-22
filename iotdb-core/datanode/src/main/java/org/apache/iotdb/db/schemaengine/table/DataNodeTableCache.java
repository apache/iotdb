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

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class DataNodeTableCache implements ITableCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeTableCache.class);

  private final Map<String, Map<String, TsTable>> databaseTableMap = new ConcurrentHashMap<>();

  private final Map<String, Map<String, TsTable>> preCreateTableMap = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private DataNodeTableCache() {
    // do nothing
  }

  private static final class DataNodeTableCacheHolder {
    private static final DataNodeTableCache INSTANCE = new DataNodeTableCache();

    private DataNodeTableCacheHolder() {}
  }

  public static DataNodeTableCache getInstance() {
    return DataNodeTableCacheHolder.INSTANCE;
  }

  @Override
  public void init(byte[] tableInitializationBytes) {
    readWriteLock.writeLock().lock();
    try {
      if (tableInitializationBytes == null) {
        return;
      }
      Pair<Map<String, List<TsTable>>, Map<String, List<TsTable>>> tableInfo =
          TsTableInternalRPCUtil.deserializeTableInitializationInfo(tableInitializationBytes);
      Map<String, List<TsTable>> usingMap = tableInfo.left;
      Map<String, List<TsTable>> preCreateMap = tableInfo.right;
      saveUpdatedTableInfo(usingMap, databaseTableMap);
      saveUpdatedTableInfo(preCreateMap, preCreateTableMap);
      LOGGER.info("Init DataNodeTableCache successfully");
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void saveUpdatedTableInfo(
      Map<String, List<TsTable>> tableMap, Map<String, Map<String, TsTable>> localTableMap) {
    for (Map.Entry<String, List<TsTable>> entry : tableMap.entrySet()) {
      Map<String, TsTable> map = new ConcurrentHashMap<>();
      for (TsTable table : entry.getValue()) {
        map.put(table.getTableName(), table);
      }
      localTableMap.put(entry.getKey(), map);
    }
  }

  @Override
  public void preCreateTable(String database, TsTable table) {
    readWriteLock.writeLock().lock();
    try {
      preCreateTableMap
          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
          .put(table.getTableName(), table);
      LOGGER.info("Pre-create table {}.{} successfully", database, table);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void rollbackCreateTable(String database, String tableName) {
    readWriteLock.writeLock().lock();
    try {
      removeTableFromPreCreateMap(database, tableName);
      LOGGER.info("Rollback-create table {}.{} successfully", database, tableName);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void removeTableFromPreCreateMap(String database, String tableName) {
    preCreateTableMap.compute(
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
  public void commitCreateTable(String database, String tableName) {
    readWriteLock.writeLock().lock();
    try {
      TsTable table = preCreateTableMap.get(database).get(tableName);
      databaseTableMap
          .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
          .put(tableName, table);
      removeTableFromPreCreateMap(database, tableName);
      LOGGER.info("Commit-create table {}.{} successfully", database, tableName);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public TsTable getTable(String database, String tableName) {
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

  public List<TsTable> getTables(String database) {
    readWriteLock.readLock().lock();
    try {
      if (databaseTableMap.containsKey(database)) {
        return new ArrayList<>(databaseTableMap.get(database).values());
      }
      return Collections.emptyList();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /** Check whether the given path overlap with some table existence. */
  public Pair<String, String> checkTableCreateAndPreCreateOnGivenPath(PartialPath path) {
    readWriteLock.writeLock().lock();
    try {
      String pathString = path.getFullPath();
      Pair<String, String> result = checkTableExistenceOnGivenPath(pathString, databaseTableMap);
      if (result == null) {
        result = checkTableExistenceOnGivenPath(pathString, preCreateTableMap);
      }
      return result;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private Pair<String, String> checkTableExistenceOnGivenPath(
      String path, Map<String, Map<String, TsTable>> tableMap) {
    int dbStartIndex = PATH_ROOT.length() + 1;
    for (Map.Entry<String, Map<String, TsTable>> dbEntry : tableMap.entrySet()) {
      String database = dbEntry.getKey();
      if (!(path.startsWith(database, dbStartIndex)
          && path.charAt(dbStartIndex + database.length()) == PATH_SEPARATOR)) {
        continue;
      }
      int tableStartIndex = dbStartIndex + database.length() + 1;
      for (String tableName : dbEntry.getValue().keySet()) {
        if (path.startsWith(tableName, tableStartIndex)
            && path.charAt(tableStartIndex + tableName.length()) == PATH_SEPARATOR) {
          return new Pair<>(database, tableName);
        }
      }
    }
    return null;
  }
}
