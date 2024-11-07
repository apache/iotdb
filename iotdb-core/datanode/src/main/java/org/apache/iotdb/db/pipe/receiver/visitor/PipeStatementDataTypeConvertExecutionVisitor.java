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
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public abstract class PipeStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, TSStatus> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeStatementDataTypeConvertExecutionVisitor.class);

  @FunctionalInterface
  public interface StatementExecutor {
    TSStatus execute(final Statement statement);
  }

  protected final StatementExecutor statementExecutor;

  public PipeStatementDataTypeConvertExecutionVisitor(final StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  protected Optional<TSStatus> tryExecute(final Statement statement) {
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
    if (status.getCode() != TSStatusCode.LOAD_FILE_ERROR.getStatusCode()
        // Ignore the error if it is caused by insufficient memory
        || (status.getMessage() != null && status.getMessage().contains("memory"))) {
      return Optional.empty();
    }

    LOGGER.warn(
        "Data type mismatch detected (TSStatus: {}) for LoadTsFileStatement: {}. Start data type conversion.",
        status,
        loadTsFileStatement);

    if (!executeConvert(loadTsFileStatement).isPresent()) {
      return Optional.empty();
    }

    if (loadTsFileStatement.isDeleteAfterLoad()) {
      loadTsFileStatement.getTsFiles().forEach(FileUtils::deleteQuietly);
    }

    LOGGER.warn(
        "Data type conversion for LoadTsFileStatement {} is successful.", loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  protected abstract Optional<TSStatus> executeConvert(
      final LoadTsFileStatement loadTsFileStatement);
}
