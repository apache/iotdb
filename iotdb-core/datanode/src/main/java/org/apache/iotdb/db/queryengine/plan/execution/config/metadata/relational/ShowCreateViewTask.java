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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ShowCreateViewTask extends AbstractTableTask {
  public ShowCreateViewTask(final String database, final String tableName) {
    super(database, tableName);
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.describeTable(database, tableName, false, true);
  }

  public static void buildTsBlock(
      final TsTable table, final SettableFuture<ConfigTaskResult> future) {
    if (!TreeViewSchema.isTreeViewTable(table)) {
      throw new SemanticException(
          "The table "
              + table.getTableName()
              + " is a base table, does not support show create view.");
    }
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showCreateViewColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    builder.getTimeColumnBuilder().writeLong(0L);
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(table.getTableName(), TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(getShowCreateViewSQL(table), TSFileConfig.STRING_CHARSET));
    builder.declarePosition();

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowCreateViewColumnHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  public static String getShowCreateViewSQL(final TsTable table) {
    final StringBuilder builder =
        new StringBuilder("CREATE TABLE VIEW \"").append(table.getTableName()).append("\" (");

    for (final TsTableColumnSchema schema : table.getColumnList()) {
      switch (schema.getColumnCategory()) {
        case TAG:
          builder
              .append(schema.getColumnName())
              .append(" ")
              .append(schema.getDataType())
              .append(" ")
              .append("TAG");
          break;
        case TIME:
          continue;
        case FIELD:
          builder
              .append(schema.getColumnName())
              .append(" ")
              .append(schema.getDataType())
              .append(" ")
              .append("FIELD");
          if (Objects.nonNull(TreeViewSchema.getOriginalName(schema))) {
            builder.append(" FROM ").append(TreeViewSchema.getOriginalName(schema));
          }
          break;
        case ATTRIBUTE:
        default:
          throw new UnsupportedOperationException(
              "Unsupported column type: " + schema.getColumnCategory());
      }
      if (Objects.nonNull(schema.getProps().get(TsTable.COMMENT_KEY))) {
        builder.append(" COMMENT ").append(schema.getProps().get(TsTable.COMMENT_KEY));
      }
      builder.append(",");
    }

    if (table.getColumnList().size() > 1) {
      builder.deleteCharAt(builder.length() - 1);
    }

    builder.append(") AS ").append(table.getPropValue(TreeViewSchema.TREE_PATH_PATTERN).get());

    if (table.getPropValue(TsTable.COMMENT_KEY).isPresent()) {
      builder.append(" COMMENT '").append(table.getPropValue(TsTable.COMMENT_KEY)).append("'");
    }
    if (table.getPropValue(TsTable.TTL_PROPERTY).isPresent()) {
      builder
          .append(" WITH (ttl=")
          .append(table.getPropValue(TsTable.TTL_PROPERTY).get())
          .append(")");
    }
    if (TreeViewSchema.isRestrict(table)) {
      builder.append(" RESTRICT");
    }
    return builder.toString();
  }
}
