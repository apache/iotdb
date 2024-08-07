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
import org.apache.iotdb.db.pipe.receiver.transform.statement.PipeConvertedInsertRowStatement;
import org.apache.iotdb.db.pipe.receiver.transform.statement.PipeConvertedInsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This visitor transforms the data type of the statement when the statement is executed and an
 * exception occurs. The transformed statement (if any) is returned and will be executed again.
 */
public class PipeStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, Exception> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeStatementDataTypeConvertExecutionVisitor.class);

  @FunctionalInterface
  public interface StatementExecutor {
    TSStatus execute(final Statement statement);
  }

  private final StatementExecutor statementExecutor;

  public PipeStatementDataTypeConvertExecutionVisitor(final StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  private Optional<TSStatus> tryExecute(final Statement statement) {
    try {
      return Optional.of(statementExecutor.execute(statement));
    } catch (final Exception e) {
      LOGGER.warn("Failed to execute statement after data type conversion.", e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<TSStatus> visitNode(
      final StatementNode statementNode, final Exception exception) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Exception exception) {
    // TODO: judge if the exception is caused by data type mismatch
    // TODO: convert the data type of the statement
    return visitStatement(loadTsFileStatement, exception);
  }

  @Override
  public Optional<TSStatus> visitInsertRow(
      final InsertRowStatement insertRowStatement, final Exception exception) {
    // TODO: judge if the exception is caused by data type mismatch

    return tryExecute(new PipeConvertedInsertRowStatement(insertRowStatement));
  }

  @Override
  public Optional<TSStatus> visitInsertRows(
      final InsertRowsStatement insertRowsStatement, final Exception exception) {
    // TODO: judge if the exception is caused by data type mismatch

    if (insertRowsStatement.getInsertRowStatementList() == null
        || insertRowsStatement.getInsertRowStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertRowsStatement convertedInsertRowsStatement = new InsertRowsStatement();
    convertedInsertRowsStatement.setInsertRowStatementList(
        insertRowsStatement.getInsertRowStatementList().stream()
            .map(PipeConvertedInsertRowStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertRowsStatement);
  }

  @Override
  public Optional<TSStatus> visitInsertRowsOfOneDevice(
      final InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement,
      final Exception exception) {
    // TODO: judge if the exception is caused by data type mismatch

    if (insertRowsOfOneDeviceStatement.getInsertRowStatementList() == null
        || insertRowsOfOneDeviceStatement.getInsertRowStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertRowsOfOneDeviceStatement convertedInsertRowsOfOneDeviceStatement =
        new InsertRowsOfOneDeviceStatement();
    convertedInsertRowsOfOneDeviceStatement.setInsertRowStatementList(
        insertRowsOfOneDeviceStatement.getInsertRowStatementList().stream()
            .map(PipeConvertedInsertRowStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertRowsOfOneDeviceStatement);
  }

  @Override
  public Optional<TSStatus> visitInsertTablet(
      final InsertTabletStatement insertTabletStatement, final Exception exception) {
    // TODO: judge if the exception is caused by data type mismatch

    return tryExecute(new PipeConvertedInsertTabletStatement(insertTabletStatement));
  }

  @Override
  public Optional<TSStatus> visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement, final Exception exception) {
    // TODO: judge if the exception is caused by data type mismatch

    if (insertMultiTabletsStatement.getInsertTabletStatementList() == null
        || insertMultiTabletsStatement.getInsertTabletStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertMultiTabletsStatement convertedInsertMultiTabletsStatement =
        new InsertMultiTabletsStatement();
    convertedInsertMultiTabletsStatement.setInsertTabletStatementList(
        insertMultiTabletsStatement.getInsertTabletStatementList().stream()
            .map(PipeConvertedInsertTabletStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertMultiTabletsStatement);
  }
}
