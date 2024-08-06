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

package org.apache.iotdb.db.pipe.receiver.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * This visitor transforms the data type of the statement when the statement is executed and an
 * exception occurs. The transformed statement (if any) is returned and will be executed again.
 */
public class PipeStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, Exception> {

  private final Consumer<Statement> statementExecutor;

  public PipeStatementDataTypeConvertExecutionVisitor(final Consumer<Statement> statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  @Override
  public Optional<TSStatus> visitNode(
      final StatementNode statementNode, final Exception exception) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Exception exception) {
    return visitStatement(loadTsFileStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsert(
      InsertStatement insertStatement, final Exception exception) {
    return visitStatement(insertStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsertTablet(
      InsertTabletStatement insertTabletStatement, final Exception exception) {
    return visitStatement(insertTabletStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsertRow(
      InsertRowStatement insertRowStatement, final Exception exception) {
    return visitStatement(insertRowStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsertRows(
      InsertRowsStatement insertRowsStatement, final Exception exception) {
    return visitStatement(insertRowsStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, final Exception exception) {
    return visitStatement(insertMultiTabletsStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, final Exception exception) {
    return visitStatement(insertRowsOfOneDeviceStatement, exception);
  }
}
