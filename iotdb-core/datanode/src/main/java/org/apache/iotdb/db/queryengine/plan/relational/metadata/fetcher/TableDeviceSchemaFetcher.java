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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableDeviceSchemaFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDeviceSchemaFetcher.class);

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();

  private final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

  private TableDeviceSchemaFetcher() {
    // do nothing
  }

  private static class TableDeviceSchemaFetcherHolder {
    private static final TableDeviceSchemaFetcher INSTANCE = new TableDeviceSchemaFetcher();
  }

  public static TableDeviceSchemaFetcher getInstance() {
    return TableDeviceSchemaFetcherHolder.INSTANCE;
  }

  TableDeviceSchemaCache getTableDeviceCache() {
    return cache;
  }

  Map<TableDeviceId, Map<String, String>> fetchMissingDeviceSchema(
      FetchDevice statement, MPPQueryContext context) {
    long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    String database = statement.getDatabase();
    String table = statement.getTableName();
    TsTable tableInstance = DataNodeTableCache.getInstance().getTable(database, table);

    ExecutionResult executionResult =
        Coordinator.getInstance()
            .executeForTableModel(
                statement,
                null,
                SessionManager.getInstance().getCurrSession(),
                queryId,
                SessionManager.getInstance()
                    .getSessionInfo(SessionManager.getInstance().getCurrSession()),
                "",
                LocalExecutionPlanner.getInstance().metadata,
                config.getQueryTimeoutThreshold());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }

    List<ColumnHeader> columnHeaderList =
        coordinator.getQueryExecution(queryId).getDatasetHeader().getColumnHeaders();
    int idLength = DataNodeTableCache.getInstance().getTable(database, table).getIdNums();
    Map<TableDeviceId, Map<String, String>> fetchedDeviceSchema = new HashMap<>();

    try {
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (IoTDBException e) {
          t = e;
          throw new RuntimeException("Fetch Table Device Schema failed. ", e);
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          break;
        }
        Column[] columns = tsBlock.get().getValueColumns();
        for (int i = 0; i < tsBlock.get().getPositionCount(); i++) {
          String[] nodes = new String[idLength];
          int idIndex = 0;
          Map<String, String> attributeMap = new HashMap<>();
          for (int j = 0; j < columnHeaderList.size(); j++) {
            TsTableColumnSchema columnSchema =
                tableInstance.getColumnSchema(columnHeaderList.get(j).getColumnName());
            if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
              if (columns[j].isNull(i)) {
                nodes[idIndex] = null;
              } else {
                nodes[idIndex] = columns[j].getBinary(i).toString();
              }
              idIndex++;
            } else {
              if (columns[j].isNull(i)) {
                attributeMap.put(columnSchema.getColumnName(), null);
              } else {
                attributeMap.put(columnSchema.getColumnName(), columns[j].getBinary(i).toString());
              }
            }
          }
          fetchedDeviceSchema.put(new TableDeviceId(nodes), attributeMap);
        }
      }
    } catch (Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return fetchedDeviceSchema;
  }
}
