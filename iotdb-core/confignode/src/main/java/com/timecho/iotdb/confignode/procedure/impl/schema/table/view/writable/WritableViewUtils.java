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

package com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AbstractAlterOrDropTableProcedure;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public class WritableViewUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(WritableViewUtils.class);

  public static boolean isIdempotent(final Exception e) {
    return isIdempotent(getErrorCode(e));
  }

  public static boolean isIdempotent(final TSStatus status) {
    return Objects.nonNull(status) && isIdempotent(status.getCode());
  }

  private static boolean isIdempotent(final int errorCode) {
    return errorCode == TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()
        || errorCode == TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode()
        || errorCode == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode();
  }

  public static int getErrorCode(final Exception e) {
    for (Throwable current = e; Objects.nonNull(current); current = current.getCause()) {
      if (current instanceof IoTDBException) {
        return ((IoTDBException) current).getErrorCode();
      }
    }
    return -1;
  }

  // Return the original table if schema cascade applies or is explicitly overridden.
  public static Pair<String, TsTable> getSourceDatabaseAndTable(
      final AbstractAlterOrDropTableProcedure<?> procedure, final Boolean schemaCascadeOverride) {
    if (Objects.isNull(procedure.getTable())) {
      try {
        final Optional<Pair<TsTable, TableNodeStatus>> tablePair =
            ConfigNode.getInstance()
                .getConfigManager()
                .getClusterSchemaManager()
                .getTableAndStatusIfExists(procedure.getDatabase(), procedure.getTableName());
        if (!tablePair.isPresent()) {
          procedure.setFailure(
              new ProcedureException(
                  new IoTDBException(
                      RpcUtils.getStatus(
                          TSStatusCode.TABLE_NOT_EXISTS,
                          String.format(
                              ProcedureMessages.TABLE_DOES_NOT_EXIST,
                              procedure.getDatabase(),
                              procedure.getTableName())))));
        }
        tablePair.ifPresent(
            tsTableTableNodeStatusPair -> procedure.setTable(tsTableTableNodeStatusPair.left));
      } catch (final MetadataException e) {
        procedure.setFailure(new ProcedureException(e));
      }
    }
    final TsTable table = procedure.getTable();
    if (!(table instanceof WritableView)) {
      return null;
    }
    final boolean shouldResolveSource =
        schemaCascadeOverride == Boolean.TRUE
            || (Objects.isNull(schemaCascadeOverride) && ((WritableView) table).isSchemaCascade());
    if (!shouldResolveSource) {
      return null;
    }
    final WritableView writableView = (WritableView) table;
    final String sourceDatabase = writableView.getSourceTableDatabase();
    final String sourceTableName = writableView.getSourceTableName();
    try {
      final Optional<Pair<TsTable, TableNodeStatus>> tablePair =
          ConfigNode.getInstance()
              .getConfigManager()
              .getClusterSchemaManager()
              .getTableAndStatusIfExists(sourceDatabase, sourceTableName);
      if (!tablePair.isPresent()) {
        LOGGER.warn(
            ProcedureMessages.SKIP_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE,
            procedure.getDatabase(),
            procedure.getTableName(),
            sourceDatabase,
            sourceTableName);
        return null;
      }
      return new Pair<>(sourceDatabase, tablePair.get().left);
    } catch (final MetadataException e) {
      if (isMissingSourceTable(e.getErrorCode())) {
        LOGGER.warn(
            ProcedureMessages.SKIP_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE_DETAIL,
            procedure.getDatabase(),
            procedure.getTableName(),
            sourceDatabase,
            sourceTableName,
            e.getMessage());
        return null;
      }
      procedure.setFailure(new ProcedureException(e));
    }
    return null;
  }

  public static void executeForSource(
      final AbstractAlterOrDropTableProcedure<?> procedure,
      final AbstractAlterOrDropTableProcedure<?> sourceProcedure,
      final Runnable function) {
    executeForSource(
        procedure,
        sourceProcedure,
        function,
        procedure.getTable() instanceof WritableView
            && ((WritableView) procedure.getTable()).isSchemaCascade());
  }

  public static void executeForSource(
      final AbstractAlterOrDropTableProcedure<?> procedure,
      final AbstractAlterOrDropTableProcedure<?> sourceProcedure,
      final Runnable function,
      final boolean shouldExecute) {
    if (Objects.isNull(sourceProcedure)) {
      return;
    }
    if (!procedure.isFailed() && shouldExecute) {
      function.run();
      if (sourceProcedure.isFailed()) {
        final int errorCode = getErrorCode(sourceProcedure.getFailure());
        if (isMissingSourceTable(errorCode)) {
          LOGGER.warn(
              ProcedureMessages.SKIP_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE_DETAIL,
              procedure.getDatabase(),
              procedure.getTableName(),
              sourceProcedure.getDatabase(),
              sourceProcedure.getTableName(),
              sourceProcedure.getFailure().getMessage());
        } else if (!isIdempotent(sourceProcedure.getFailure())) {
          procedure.setFailure(sourceProcedure.getFailure());
        }
      }
    }
  }

  private static boolean isMissingSourceTable(final int errorCode) {
    return errorCode == TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()
        || errorCode == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode();
  }

  private WritableViewUtils() {
    // Private constructor
  }
}
