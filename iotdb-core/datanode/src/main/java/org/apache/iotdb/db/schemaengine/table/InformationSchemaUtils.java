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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.table.TableNotExistsException;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;
import static org.apache.iotdb.commons.schema.table.InformationSchema.getSchemaTables;

public class InformationSchemaUtils {

  public static void checkDBNameInWrite(final String dbName) {
    if (dbName.equals(INFORMATION_DATABASE)) {
      throw new SemanticException(
          new IoTDBException(
              "The database 'information_schema' can only be queried",
              TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
    }
  }

  public static void buildDatabaseTsBlock(
      final TsBlockBuilder builder, final boolean details, final boolean withTime) {
    if (withTime) {
      builder.getTimeColumnBuilder().writeLong(0L);
    }
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(INFORMATION_DATABASE, TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(IoTDBConstant.TTL_INFINITE, TSFileConfig.STRING_CHARSET));

    builder.getColumnBuilder(2).appendNull();
    builder.getColumnBuilder(3).appendNull();
    builder.getColumnBuilder(4).appendNull();
    if (details) {
      builder.getColumnBuilder(5).appendNull();
      builder.getColumnBuilder(6).appendNull();
    }
    builder.declarePosition();
  }

  public static TsTable mayGetTable(final String database, final String tableName) {
    return INFORMATION_DATABASE.equals(database) ? getSchemaTables().get(tableName) : null;
  }

  public static boolean mayUseDB(
      final String database,
      final IClientSession clientSession,
      final SettableFuture<ConfigTaskResult> future) {
    if (!database.equals(INFORMATION_DATABASE)) {
      return false;
    }
    clientSession.setDatabaseName(INFORMATION_DATABASE);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    return true;
  }

  public static boolean mayShowTable(
      final String database,
      final boolean isDetails,
      final SettableFuture<ConfigTaskResult> future) {
    if (!database.equals(INFORMATION_DATABASE)) {
      return false;
    }
    final List<TSDataType> outputDataTypes =
        (isDetails
                ? ColumnHeaderConstant.showTablesDetailsColumnHeaders
                : ColumnHeaderConstant.showTablesColumnHeaders)
            .stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final String schemaTable :
        getSchemaTables().keySet().stream().sorted().collect(Collectors.toList())) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(schemaTable, TSFileConfig.STRING_CHARSET));
      builder.getColumnBuilder(1).writeBinary(new Binary("INF", TSFileConfig.STRING_CHARSET));
      if (isDetails) {
        builder.getColumnBuilder(2).writeBinary(new Binary("USING", TSFileConfig.STRING_CHARSET));
        builder.getColumnBuilder(3).appendNull();
        // Does not support comment
        builder
            .getColumnBuilder(4)
            .writeBinary(new Binary(TableType.SYSTEM_VIEW.getName(), TSFileConfig.STRING_CHARSET));
      }
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            isDetails
                ? DatasetHeaderFactory.getShowTablesDetailsHeader()
                : DatasetHeaderFactory.getShowTablesHeader()));
    return true;
  }

  public static boolean mayDescribeTable(
      final String database,
      final String tableName,
      final boolean isDetails,
      final Boolean isShowOrCreateView,
      final SettableFuture<ConfigTaskResult> future) {
    if (!database.equals(INFORMATION_DATABASE)) {
      return false;
    }
    if (!getSchemaTables().containsKey(tableName)) {
      final TableNotExistsException exception =
          new TableNotExistsException(INFORMATION_DATABASE, tableName);
      future.setException(new IoTDBException(exception.getMessage(), exception.getErrorCode()));
      return true;
    }
    if (Objects.nonNull(isShowOrCreateView)) {
      throw new SemanticException("The system view does not support show create.");
    }
    final TsTable table = getSchemaTables().get(tableName);

    final List<TSDataType> outputDataTypes =
        (isDetails
                ? ColumnHeaderConstant.describeTableDetailsColumnHeaders
                : ColumnHeaderConstant.describeTableColumnHeaders)
            .stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final TsTableColumnSchema columnSchema : table.getColumnList()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(new Binary(columnSchema.getColumnName(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(1)
          .writeBinary(new Binary(columnSchema.getDataType().name(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(2)
          .writeBinary(
              new Binary(columnSchema.getColumnCategory().name(), TSFileConfig.STRING_CHARSET));
      if (isDetails) {
        builder.getColumnBuilder(3).writeBinary(new Binary("USING", TSFileConfig.STRING_CHARSET));
        builder.getColumnBuilder(4).appendNull();
      }
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            isDetails
                ? DatasetHeaderFactory.getDescribeTableDetailsHeader()
                : DatasetHeaderFactory.getDescribeTableHeader()));
    return true;
  }

  private InformationSchemaUtils() {
    // Util class
  }
}
