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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.internal.CreateTableDeviceStatement;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.List;

public class TableModelSchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static class TableModelSchemaFetcherHolder {
    private static final TableModelSchemaFetcher INSTANCE = new TableModelSchemaFetcher();
  }

  public static TableModelSchemaFetcher getInstance() {
    return TableModelSchemaFetcherHolder.INSTANCE;
  }

  private TableModelSchemaFetcher() {
    // do nothing
  }

  public void validateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation, MPPQueryContext context) {
    CreateTableDeviceStatement statement =
        new CreateTableDeviceStatement(
            schemaValidation.getDatabase(),
            schemaValidation.getTableName(),
            schemaValidation.getDeviceIdList(),
            schemaValidation.getAttributeColumnNameList(),
            schemaValidation.getAttributeValue());
    ExecutionResult executionResult =
        Coordinator.getInstance()
            .executeForTreeModel(
                statement,
                SessionManager.getInstance().requestQueryId(),
                context == null ? null : context.getSession(),
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                context == null || context.getQueryType().equals(QueryType.WRITE)
                    ? config.getQueryTimeoutThreshold()
                    : context.getTimeOut());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }
  }

  public List<DeviceEntry> fetchDeviceSchema(
      String database,
      String table,
      List<Expression> expressionList,
      List<String> attributeColumns) {
    return null;
  }
}
