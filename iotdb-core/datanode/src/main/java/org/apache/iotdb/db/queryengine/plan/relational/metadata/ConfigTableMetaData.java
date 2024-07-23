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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.DATABASE;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.DATA_REGION_NUM;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.DATA_REPLICATION_FACTOR;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.INTERNAL_ADDRESS;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.INTERNAL_PORT;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.NODE_ID;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ROLE;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.RPC_ADDRESS;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.RPC_PORT;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.SCHEMA_REGION_NUM;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.STATUS;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.TIME_PARTITION_INTERVAL;

public class ConfigTableMetaData {
  private static final Map<String, Pair<TableSchema, Supplier<SettableFuture<ConfigTaskResult>>>>
      CONFIG_TABLE_MAP = new HashMap<>();

  static {
    CONFIG_TABLE_MAP.put(
        "databases",
        new Pair<>(
            new TableSchema(
                "databases",
                Arrays.asList(
                    new ColumnSchema(
                        DATABASE,
                        TypeFactory.getType(TSDataType.STRING),
                        false,
                        TsTableColumnCategory.ID),
                    new ColumnSchema(
                        SCHEMA_REPLICATION_FACTOR,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        DATA_REPLICATION_FACTOR,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        TIME_PARTITION_INTERVAL,
                        TypeFactory.getType(TSDataType.INT64),
                        false,
                        TsTableColumnCategory.MEASUREMENT))),
            () -> ClusterConfigTaskExecutor.getInstance().showDatabases(new ShowDB())));
    CONFIG_TABLE_MAP.put(
        "datanodes",
        new Pair<>(
            new TableSchema(
                "datanodes",
                Arrays.asList(
                    new ColumnSchema(
                        NODE_ID,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.ID),
                    new ColumnSchema(
                        STATUS,
                        TypeFactory.getType(TSDataType.TEXT),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        RPC_ADDRESS,
                        TypeFactory.getType(TSDataType.TEXT),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        RPC_PORT,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        DATA_REGION_NUM,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        SCHEMA_REGION_NUM,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.MEASUREMENT))),
            () -> ClusterConfigTaskExecutor.getInstance().showDataNodes(new ShowDataNodes())));
    CONFIG_TABLE_MAP.put(
        "confignodes",
        new Pair<>(
            new TableSchema(
                "confignodes",
                Arrays.asList(
                    new ColumnSchema(
                        NODE_ID,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.ID),
                    new ColumnSchema(
                        STATUS,
                        TypeFactory.getType(TSDataType.TEXT),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        INTERNAL_ADDRESS,
                        TypeFactory.getType(TSDataType.TEXT),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        INTERNAL_PORT,
                        TypeFactory.getType(TSDataType.INT32),
                        false,
                        TsTableColumnCategory.MEASUREMENT),
                    new ColumnSchema(
                        ROLE,
                        TypeFactory.getType(TSDataType.TEXT),
                        false,
                        TsTableColumnCategory.MEASUREMENT))),
            () -> ClusterConfigTaskExecutor.getInstance().showConfigNodes(new ShowConfigNodes())));
  }

  public static Optional<TableSchema> getTableSchema(final String tableName) {
    return Optional.ofNullable(CONFIG_TABLE_MAP.get(tableName).getLeft());
  }

  public static TsBlock getTsBlock(final String tableName) throws Exception {
    return CONFIG_TABLE_MAP.get(tableName).getRight().get().get().getResultSet();
  }

  private ConfigTableMetaData() {
    // Utility class
  }
}
