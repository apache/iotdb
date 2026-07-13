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

import org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.schema.table.NonCommittableTsTable;
import org.apache.iotdb.commons.schema.table.PreDeleteTsTable;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeLeaseRecoveryResp;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.i18n.DataNodeSchemaMessages.FAILED_TO_REFRESH_CACHE_FROM_CN;

/** It contains all tables' latest column schema */
public class DataNodeTableCache implements ITableCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeTableCache.class);
  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  /** Instance-specific version counter for optimistic locking mechanisms. */
  private final AtomicLong instanceVersion = new AtomicLong(0);

  // The database is without "root"
  private final Map<String, Map<String, TsTable>> databaseTableMap = new ConcurrentHashMap<>();

  // The database is without "root"
  private final Map<String, Map<String, Pair<TsTable, Long>>> specialStatusMap =
      new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Semaphore fetchTableSemaphore =
      new Semaphore(
          IoTDBDescriptor.getInstance().getConfig().getDataNodeTableCacheSemaphorePermitNum());

  private DataNodeTableCache() {}

  private static final class DataNodeTableCacheHolder {
    private static final DataNodeTableCache INSTANCE = new DataNodeTableCache();

    private DataNodeTableCacheHolder() {}
  }

  public static ITableCache getInstance() {
    return DataNodeTableCacheHolder.INSTANCE;
  }

  void failIfMetadataLeaseFenced() {
    MetadataLeaseManager.getInstance().failIfMetadataLeaseFenced();
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
      final Map<String, List<TsTable>> specialStatusMap = tableInfo.right;
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
      specialStatusMap.forEach(
          (key, value) ->
              this.specialStatusMap.put(
                  PathUtils.unQualifyDatabaseName(key),
                  value.stream()
                      .collect(
                          Collectors.toMap(
                              TsTable::getTableName,
                              table -> new Pair<>(table, 0L),
                              (v1, v2) -> v2,
                              ConcurrentHashMap::new))));
      LOGGER.info(DataNodeSchemaMessages.INIT_TABLE_CACHE_SUCCESS);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // No need to acquire a lock here; reloadTableCacheAfterLeaseRecovery is within a critical section
  // protected by metadataLeaseManager
  @Override
  public void reloadTableCacheAfterLeaseRecovery() {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TDataNodeLeaseRecoveryResp resp = configNodeClient.reloadCacheAfterLeaseRecovery();
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBRuntimeException(resp.getStatus().getMessage(), resp.getStatus().getCode());
      }
      if (resp.isSetTableInfo()) {
        init(resp.getTableInfo());
      }
    } catch (final ClientManagerException | TException e) {
      throw new RuntimeException(FAILED_TO_REFRESH_CACHE_FROM_CN, e);
    }
  }

  /**
   * The case that pre update Table and pre delete Table procedures targeting the same table are
   * executed serially by CN.
   *
   * <p>Consider the scenario:
   *
   * <ol>
   *   <li>Drop the table first: DN executed pre update but missed the commit phase
   *   <li>Create table second: DN executed pre update, which overwrites the result of the drop
   *       table procedure in {@link #specialStatusMap}
   * </ol>
   */
  @Override
  public void preUpdateTable(String database, final TsTable table, final String oldName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      failIfMetadataLeaseFenced();
      specialStatusMap
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
      LOGGER.info(DataNodeSchemaMessages.PRE_UPDATE_TABLE_SUCCESS, database, table.getTableName());
      if (table instanceof PreDeleteTsTable) {
        if (databaseTableMap.containsKey(database)) {
          databaseTableMap.get(database).remove(table.getTableName());
        }
      }
      // If rename table
      if (Objects.nonNull(oldName)) {
        final TsTable oldTable = databaseTableMap.get(database).remove(oldName);
        specialStatusMap
            .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
            .compute(
                oldName,
                (k, v) -> {
                  if (Objects.isNull(v)) {
                    return new Pair<>(oldTable, 0L);
                  } else {
                    v.setLeft(oldTable);
                    v.setRight(v.getRight() + 1);
                    return v;
                  }
                });
        LOGGER.info(DataNodeSchemaMessages.PRE_RENAME_OLD_TABLE_SUCCESS, database, oldName);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void rollbackUpdateTable(String database, final String tableName, final String oldName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      failIfMetadataLeaseFenced();
      // if rollback the drop table procedure, do nothing,
      // wait for triggering the action of pull table from CN
      final TsTable table = getTableFromSpecialStatusMap(database, tableName);
      if (table instanceof PreDeleteTsTable) {
        return;
      }
      removeTableFromSpecialStatusMap(database, tableName);
      LOGGER.info(DataNodeSchemaMessages.ROLLBACK_UPDATE_TABLE_SUCCESS, database, tableName);

      // If rename table
      if (Objects.nonNull(oldName)) {
        // Equals to commit update
        final TsTable oldTable = getTableFromSpecialStatusMap(database, oldName);
        if (Objects.isNull(oldTable)) {
          LOGGER.info(
              DataNodeSchemaMessages
                  .MESSAGE_SKIP_ROLLBACK_RENAMING_OLD_TABLE_ARG_ARG_BECAUSE_IT_HAS_BEEN_HANDLED_664F2456,
              database,
              oldName);
          return;
        }
        // Cannot be rolled back, consider:
        // 1. Fetched a written CN table
        // 2. CN rollback because of timeout
        // 3. If we roll back here, the flag will be cleared, and it will always be the written
        // one
        if (oldTable instanceof NonCommittableTsTable) {
          return;
        }
        databaseTableMap
            .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
            .put(oldName, oldTable);
        LOGGER.info(DataNodeSchemaMessages.ROLLBACK_RENAME_OLD_TABLE_SUCCESS, database, oldName);
        removeTableFromSpecialStatusMap(database, oldName);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private @Nullable TsTable getTableFromSpecialStatusMap(
      final String database, final String tableName) {
    final Map<String, Pair<TsTable, Long>> tableMap = specialStatusMap.get(database);
    if (Objects.isNull(tableMap)) {
      return null;
    }
    final Pair<TsTable, Long> tableVersionPair = tableMap.get(tableName);
    return Objects.nonNull(tableVersionPair) ? tableVersionPair.getLeft() : null;
  }

  private void removeTableFromSpecialStatusMap(final String database, final String tableName) {
    specialStatusMap.computeIfPresent(
        database,
        (k, v) -> {
          v.computeIfPresent(
              tableName,
              (innerKey, tableVersionPair) -> {
                tableVersionPair.setLeft(null);
                return tableVersionPair;
              });
          return v;
        });
  }

  @Override
  public void commitUpdateTable(
      String database, final String tableName, final @Nullable String oldName) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      failIfMetadataLeaseFenced();
      final TsTable newTable = getTableFromSpecialStatusMap(database, tableName);
      if (Objects.isNull(newTable)) {
        LOGGER.info(
            DataNodeSchemaMessages
                .MESSAGE_SKIP_COMMIT_UPDATE_TABLE_ARG_ARG_BECAUSE_IT_HAS_BEEN_HANDLED_31362A1C,
            database,
            tableName);
        if (Objects.nonNull(oldName)) {
          removeTableFromSpecialStatusMap(database, oldName);
        }
        return;
      }
      // Cannot be committed, consider:
      // 1. Fetched a non-changed CN table
      // 2. CN is changed
      // 3. If we commit here, it will always be the non-changed one
      // (And it is not committable because it's not real table)
      if (newTable instanceof NonCommittableTsTable) {
        return;
      }
      if (newTable instanceof PreDeleteTsTable) {
        commitDeleteTable(database, tableName);
        return;
      }
      final TsTable oldTable =
          databaseTableMap
              .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
              .put(tableName, newTable);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            DataNodeSchemaMessages.COMMIT_UPDATE_TABLE_SUCCESS_WITH_DETAIL,
            database,
            tableName,
            compareTable(oldTable, newTable));
      } else if (LOGGER.isInfoEnabled()) {
        LOGGER.info(DataNodeSchemaMessages.COMMIT_UPDATE_TABLE_SUCCESS, database, tableName);
      }
      removeTableFromSpecialStatusMap(database, tableName);
      if (Objects.nonNull(oldName)) {
        removeTableFromSpecialStatusMap(database, oldName);
        LOGGER.info(DataNodeSchemaMessages.RENAME_OLD_TABLE_SUCCESS, database, oldName);
      }
      instanceVersion.incrementAndGet();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void commitDeleteTable(String database, final String tableName) {
    if (databaseTableMap.containsKey(database)) {
      databaseTableMap.get(database).remove(tableName);
    }
    removeTableFromSpecialStatusMap(database, tableName);
    LOGGER.info(DataNodeSchemaMessages.COMMIT_DELETE_TABLE_SUCCESS, database, tableName);
  }

  @Override
  public void invalid(String database) {
    database = PathUtils.unQualifyDatabaseName(database);
    readWriteLock.writeLock().lock();
    try {
      databaseTableMap.remove(database);
      specialStatusMap.remove(database);
      instanceVersion.incrementAndGet();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Drop the entire cache. Used on metadata-lease recovery: after the DataNode was fenced it may
   * have missed ConfigNode pushes, so the cached schema is no longer trustworthy and must be
   * re-fetched lazily on the next lookup.
   */
  @Override
  public void invalidateAll() {
    readWriteLock.writeLock().lock();
    try {
      databaseTableMap.clear();
      specialStatusMap.clear();
      instanceVersion.incrementAndGet();
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
        final TsTable copyTable = new TsTable(databaseTableMap.get(database).get(tableName));
        copyTable.removeColumnSchema(columnName);
        databaseTableMap.get(database).put(tableName, copyTable);
      }
      if (specialStatusMap.containsKey(database)
          && specialStatusMap.get(database).containsKey(tableName)) {
        final Pair<TsTable, Long> tableVersionPair = specialStatusMap.get(database).get(tableName);
        if (Objects.nonNull(tableVersionPair.getLeft())) {
          final TsTable copyTable = new TsTable(tableVersionPair.getLeft());
          copyTable.removeColumnSchema(columnName);
          tableVersionPair.setLeft(copyTable);
        }
        tableVersionPair.setRight(tableVersionPair.getRight() + 1);
      }
      instanceVersion.incrementAndGet();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public long getInstanceVersion() {
    return instanceVersion.get();
  }

  @Override
  public Map<String, Map<String, TsTable>> getTableSnapshot() {
    readWriteLock.readLock().lock();
    try {
      failIfMetadataLeaseFenced();
      return databaseTableMap.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  entry ->
                      entry.getValue().entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey,
                                  tableEntry -> new TsTable(tableEntry.getValue()),
                                  (left, right) -> right,
                                  HashMap::new)),
                  (left, right) -> right,
                  HashMap::new));
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public TsTable getTableInWrite(final String database, final String tableName) {
    final TsTable result = getTableInCache(database, tableName);
    return Objects.nonNull(result) ? result : getTable(database, tableName, false);
  }

  @Override
  public TsTable getTable(final String database, final String tableName) {
    return getTable(database, tableName, true);
  }

  /**
   * The following logic can handle the cases when configNode failed to clear some table in {@link
   * #specialStatusMap}, due to the failure of "commit" or rollback of "pre-update".
   */
  @Override
  public TsTable getTable(String database, final String tableName, final boolean force) {
    database = PathUtils.unQualifyDatabaseName(database);
    final AtomicReference<TableNodeStatus> tableStatusRef = new AtomicReference<>();
    final Map<String, Map<String, Long>> specialStatusMap =
        mayGetTableInSpecialStatusMap(database, tableName, tableStatusRef);

    if (Objects.nonNull(specialStatusMap) && !specialStatusMap.isEmpty()) {
      Map<String, Map<String, TsTable>> fetchedTables =
          getTablesInConfigNode(specialStatusMap, tableStatusRef.get());
      if (tableStatusRef.get() == TableNodeStatus.USING) {
        updateUsingTable(fetchedTables, specialStatusMap);
      } else {
        updateDeleteTable(fetchedTables, database, tableName);
      }
    }
    final TsTable table = getTableInCache(database, tableName);
    if (Objects.isNull(table) && force) {
      CommonMetadataUtils.throwTableNotExistsException(database, tableName);
    }
    return table;
  }

  private Map<String, Map<String, Long>> mayGetTableInSpecialStatusMap(
      final String database,
      final String tableName,
      final AtomicReference<TableNodeStatus> tableNodeStatus) {
    readWriteLock.readLock().lock();
    try {
      failIfMetadataLeaseFenced();
      final Map<String, Pair<TsTable, Long>> targetDatabaseMap = specialStatusMap.get(database);
      if (Objects.isNull(targetDatabaseMap)) {
        return null;
      }

      final Pair<TsTable, Long> targetTablePair = targetDatabaseMap.get(tableName);
      if (Objects.isNull(targetTablePair) || Objects.isNull(targetTablePair.getLeft())) {
        return null;
      }
      final boolean targetIsPreDelete = targetTablePair.getLeft() instanceof PreDeleteTsTable;
      final Map<String, Map<String, Long>> result = new HashMap<>();
      for (final Map.Entry<String, Map<String, Pair<TsTable, Long>>> databaseEntry :
          specialStatusMap.entrySet()) {
        final Map<String, Long> tableVersionMap =
            getSpecificStatusTable(databaseEntry, targetIsPreDelete);
        if (!tableVersionMap.isEmpty()) {
          result.put(databaseEntry.getKey(), tableVersionMap);
        }
      }
      tableNodeStatus.set(targetIsPreDelete ? TableNodeStatus.PRE_DELETE : TableNodeStatus.USING);
      return result;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  private Map<String, Long> getSpecificStatusTable(
      Map.Entry<String, Map<String, Pair<TsTable, Long>>> databaseEntry,
      boolean targetIsPreDelete) {
    final Map<String, Long> tableVersionMap = new HashMap<>();
    for (final Map.Entry<String, Pair<TsTable, Long>> tableEntry :
        databaseEntry.getValue().entrySet()) {
      final TsTable candidate = tableEntry.getValue().getLeft();
      if (Objects.isNull(candidate)) {
        continue;
      }
      if ((candidate instanceof PreDeleteTsTable) == targetIsPreDelete) {
        tableVersionMap.put(tableEntry.getKey(), tableEntry.getValue().getRight());
      }
    }
    return tableVersionMap;
  }

  private Map<String, Map<String, TsTable>> getTablesInConfigNode(
      final Map<String, Map<String, Long>> tableInput, final TableNodeStatus tableNodeStatus) {
    Map<String, Map<String, TsTable>> result = Collections.emptyMap();
    boolean acquired = false;
    try {
      fetchTableSemaphore.acquire();
      acquired = true;
      final TFetchTableResp resp =
          ClusterConfigTaskExecutor.getInstance()
              .fetchTables(
                  tableInput.entrySet().stream()
                      .collect(
                          Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().keySet())),
                  tableNodeStatus);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
        result = TsTableInternalRPCUtil.deserializeTsTableFetchResult(resp.getTableInfoMap());
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(DataNodeSchemaMessages.INTERRUPTED_ACQUIRE_SEMAPHORE_GET_TABLES);
    } finally {
      if (acquired) {
        fetchTableSemaphore.release();
      }
    }
    return result;
  }

  private void updateUsingTable(
      final Map<String, Map<String, TsTable>> fetchedTables,
      final Map<String, Map<String, Long>> previousVersions) {
    readWriteLock.writeLock().lock();
    try {
      failIfMetadataLeaseFenced();
      final AtomicBoolean isUpdated = new AtomicBoolean(false);
      fetchedTables.forEach(
          (qualifiedDatabase, tableInfoMap) -> {
            final String database = PathUtils.unQualifyDatabaseName(qualifiedDatabase);
            if (specialStatusMap.containsKey(database)) {
              tableInfoMap.forEach(
                  (tableName, tsTable) -> {
                    final Pair<TsTable, Long> existingPair =
                        specialStatusMap.get(database).get(tableName);
                    if (Objects.isNull(existingPair)
                        || Objects.isNull(existingPair.getLeft())
                        || !Objects.equals(
                            existingPair.getRight(),
                            previousVersions.get(database).get(tableName))) {
                      return;
                    }
                    isUpdated.set(true);
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug(
                          DataNodeSchemaMessages.UPDATE_TABLE_BY_FETCH_WITH_DETAIL,
                          database,
                          tableName,
                          compareTable(
                              existingPair.getLeft(),
                              databaseTableMap
                                  .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
                                  .get(tableName)));
                    } else if (LOGGER.isInfoEnabled()) {
                      LOGGER.info(
                          DataNodeSchemaMessages.UPDATE_TABLE_BY_FETCH, database, tableName);
                    }
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
        instanceVersion.incrementAndGet();
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /** fetch the pre delete table to update */
  private void updateDeleteTable(
      Map<String, Map<String, TsTable>> fetchedTables,
      String targetDatabase,
      final String targetTable) {
    readWriteLock.writeLock().lock();
    try {
      failIfMetadataLeaseFenced();
      boolean isUpdated = false;
      boolean targetTableIsStillDeleting = false;

      for (final Map.Entry<String, Map<String, TsTable>> databaseEntry : fetchedTables.entrySet()) {
        final String currentDatabase = PathUtils.unQualifyDatabaseName(databaseEntry.getKey());

        final Map<String, Pair<TsTable, Long>> existingDatabaseMap =
            this.specialStatusMap.get(currentDatabase);
        if (Objects.isNull(existingDatabaseMap)) {
          continue;
        }
        for (final Map.Entry<String, TsTable> tableEntry : databaseEntry.getValue().entrySet()) {
          final String currentTableName = tableEntry.getKey();

          final Pair<TsTable, Long> existingPair = existingDatabaseMap.get(currentTableName);
          if (Objects.isNull(existingPair)
              || Objects.isNull(existingPair.getLeft())
              || !(existingPair.getLeft() instanceof PreDeleteTsTable)) {
            continue;
          }

          final TsTable fetchedTable = tableEntry.getValue();
          // case 1. the table is still in the pre delete status, do not update
          // and only remind user of it
          // the CN may be still in drop table procedure or has finished the procedure with error
          if (fetchedTable instanceof PreDeleteTsTable) {
            if (targetDatabase.equals(currentDatabase) && targetTable.equals(currentTableName)) {
              targetTableIsStillDeleting = true;
            }
            continue;
          }

          isUpdated = true;
          // case 2. the TsTable is normal TsTable, means that the drop table procedure rollback
          // recovery it in databaseTableMap
          if (Objects.nonNull(fetchedTable)) {
            databaseTableMap
                .computeIfAbsent(currentDatabase, k -> new ConcurrentHashMap<>())
                .put(currentTableName, fetchedTable);
          } else if (databaseTableMap.containsKey(currentDatabase)) {
            // case 3. the CN do not hold the table, means that the table has been deleted
            databaseTableMap.get(currentDatabase).remove(currentTableName);
          }
          // case 2 and case 3, remove table from specialStatusMap
          existingPair.setLeft(null);
        }
      }
      if (isUpdated) {
        instanceVersion.incrementAndGet();
      }
      if (targetTableIsStillDeleting) {
        throw new SemanticException(
            String.format(
                DataNodeSchemaMessages.THE_TABLE_IS_IN_PRE_DELETE_STATE,
                targetDatabase,
                targetTable));
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private String compareTable(final TsTable oldTable, final TsTable newTable) {
    if (Objects.isNull(oldTable)) {
      return DataNodeSchemaMessages.COMPARE_TABLE_ADDED + newTable;
    }
    if (Objects.isNull(newTable)) {
      return DataNodeSchemaMessages.COMPARE_TABLE_REMOVED + oldTable;
    }
    boolean modified = false;
    final StringBuilder builder =
        new StringBuilder(DataNodeSchemaMessages.COMPARE_TABLE_NAME + oldTable.getTableName());
    final Map<String, String> oldProps =
        Objects.nonNull(oldTable.getProps())
            ? new HashMap<>(oldTable.getProps())
            : Collections.emptyMap();
    final Map<String, String> newProps =
        Objects.nonNull(newTable.getProps())
            ? new HashMap<>(newTable.getProps())
            : Collections.emptyMap();
    if (!Objects.equals(oldProps, newProps)) {
      oldProps
          .keySet()
          .removeIf(
              key -> {
                if (Objects.equals(oldProps.get(key), newProps.get(key))) {
                  newProps.remove(key);
                  return true;
                }
                return false;
              });
      if (!oldProps.isEmpty()) {
        builder.append(DataNodeSchemaMessages.COMPARE_TABLE_REMOVED_PROPS).append(oldProps);
      }
      if (!newProps.isEmpty()) {
        builder.append(DataNodeSchemaMessages.COMPARE_TABLE_ADDED_PROPS).append(newProps);
      }
      modified = true;
    }

    final List<TsTableColumnSchema> oldSchema =
        oldTable.getColumnList().stream()
            .filter(
                columnSchema ->
                    Objects.isNull(newTable.getColumnSchema(columnSchema.getColumnName()))
                        || !Objects.equals(
                            columnSchema.getColumnCategory(),
                            newTable
                                .getColumnSchema(columnSchema.getColumnName())
                                .getColumnCategory())
                        || !Objects.equals(
                            columnSchema.getProps(),
                            newTable.getColumnSchema(columnSchema.getColumnName()).getProps()))
            .collect(Collectors.toList());
    final List<TsTableColumnSchema> newSchema =
        newTable.getColumnList().stream()
            .filter(
                columnSchema ->
                    Objects.isNull(oldTable.getColumnSchema(columnSchema.getColumnName()))
                        || !Objects.equals(
                            columnSchema.getColumnCategory(),
                            oldTable
                                .getColumnSchema(columnSchema.getColumnName())
                                .getColumnCategory())
                        || !Objects.equals(
                            columnSchema.getProps(),
                            oldTable.getColumnSchema(columnSchema.getColumnName()).getProps()))
            .collect(Collectors.toList());

    if (!oldSchema.isEmpty()) {
      builder.append(DataNodeSchemaMessages.COMPARE_TABLE_REMOVED_COLUMNS).append(oldSchema);
      modified = true;
    }
    if (!newSchema.isEmpty()) {
      builder.append(DataNodeSchemaMessages.COMPARE_TABLE_ADDED_COLUMNS).append(newSchema);
      modified = true;
    }
    return modified ? builder.toString() : DataNodeSchemaMessages.COMPARE_TABLE_NOT_MODIFIED;
  }

  private TsTable getTableInCache(final String database, final String tableName) {
    readWriteLock.readLock().lock();
    try {
      failIfMetadataLeaseFenced();
      final TsTable result =
          databaseTableMap.containsKey(database)
              ? databaseTableMap.get(database).get(tableName)
              : null;
      return Objects.nonNull(result)
          ? result
          : InformationSchemaUtils.mayGetTable(database, tableName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public boolean isDatabaseExist(final String database) {
    failIfMetadataLeaseFenced();
    if (databaseTableMap.containsKey(database)) {
      return true;
    }
    if (getTablesInConfigNode(
            Collections.singletonMap(database, Collections.emptyMap()), TableNodeStatus.USING)
        .containsKey(database)) {
      readWriteLock.readLock().lock();
      try {
        failIfMetadataLeaseFenced();
        databaseTableMap.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
        return true;
      } finally {
        readWriteLock.readLock().unlock();
      }
    }
    return false;
  }

  // Database shall not start with "root"
  @Override
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
}
