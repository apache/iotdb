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
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
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
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This visitor transforms the data type of the statement when the statement is executed and an
 * exception occurs. The transformed statement (if any) is returned and will be executed again.
 */
public class PipeStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, TSStatus> {

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
  public Optional<TSStatus> visitNode(final StatementNode statementNode, final TSStatus status) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final TSStatus status) {
    // TODO: judge if the exception is caused by data type mismatch
    // TODO: convert the data type of the statement
    return visitStatement(loadTsFileStatement, status);
  }

  @Override
  public Optional<TSStatus> visitInsertRow(
      final InsertRowStatement insertRowStatement, final TSStatus status) {
    return status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
            && status.getMessage() != null
            && status.getMessage().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING)
        ? tryExecute(new PipeConvertedInsertRowStatement(insertRowStatement))
        : Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitInsertRows(
      final InsertRowsStatement insertRowsStatement, final TSStatus status) {
    if (!((status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
            || status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode())
        && status.toString().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING))) {
      return Optional.empty();
    }

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
      final InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, final TSStatus status) {
    if (!((status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
            || status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode())
        && status.toString().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING))) {
      return Optional.empty();
    }

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
      final InsertTabletStatement insertTabletStatement, final TSStatus status) {
    return status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
            && status.getMessage() != null
            && status.getMessage().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING)
        ? tryExecute(new PipeConvertedInsertTabletStatement(insertTabletStatement))
        : Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement, final TSStatus status) {
    if (!((status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
            || status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode())
        && status.toString().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING))) {
      return Optional.empty();
    }

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
