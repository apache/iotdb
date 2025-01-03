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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadTsFileDataTypeConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileDataTypeConverter.class);

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final SqlParser relationalSqlParser = new SqlParser();
  private final LoadTableStatementDataTypeConvertExecutionVisitor
      tableStatementDataTypeConvertExecutionVisitor =
          new LoadTableStatementDataTypeConvertExecutionVisitor(
              ((statement, databaseName) ->
                  Coordinator.getInstance()
                      .executeForTableModel(
                          statement,
                          relationalSqlParser,
                          SESSION_MANAGER.getCurrSession(),
                          SESSION_MANAGER.requestQueryId(),
                          SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                              SESSION_MANAGER.getCurrSession(), databaseName),
                          "",
                          LocalExecutionPlanner.getInstance().metadata,
                          IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
                      .status));
  private final LoadTreeStatementDataTypeConvertExecutionVisitor
      treeStatementDataTypeConvertExecutionVisitor =
          new LoadTreeStatementDataTypeConvertExecutionVisitor(
              statement ->
                  Coordinator.getInstance()
                      .executeForTreeModel(
                          statement,
                          SESSION_MANAGER.requestQueryId(),
                          SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
                          "",
                          ClusterPartitionFetcher.getInstance(),
                          ClusterSchemaFetcher.getInstance(),
                          IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                          false)
                      .status);

  public static final LoadConvertedInsertTabletStatementTSStatusVisitor STATEMENT_STATUS_VISITOR =
      new LoadConvertedInsertTabletStatementTSStatusVisitor();
  public static final LoadConvertedInsertTabletStatementExceptionVisitor
      STATEMENT_EXCEPTION_VISITOR = new LoadConvertedInsertTabletStatementExceptionVisitor();

  public TSStatus convertForTableModel(LoadTsFile loadTsFileTableStatement) {
    try {
      return loadTsFileTableStatement
          .accept(
              tableStatementDataTypeConvertExecutionVisitor, loadTsFileTableStatement.getDatabase())
          .orElse(null);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to convert data types for table model statement {}.",
          loadTsFileTableStatement,
          e);
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus convertForTreeModel(LoadTsFileStatement loadTsFileTreeStatement) {
    try {
      return loadTsFileTreeStatement
          .accept(treeStatementDataTypeConvertExecutionVisitor, null)
          .orElse(null);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to convert data types for tree model statement {}.", loadTsFileTreeStatement, e);
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }
}
