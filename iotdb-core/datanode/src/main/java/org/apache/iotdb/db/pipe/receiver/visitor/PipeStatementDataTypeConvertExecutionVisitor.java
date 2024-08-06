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

  //
  //  @Override
  //  public TSStatus visitLoadFile(
  //      final LoadTsFileStatement loadTsFileStatement, final Exception context) {
  //    if (context instanceof LoadRuntimeOutOfMemoryException) {
  //      return new TSStatus(
  //              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
  //          .setMessage(context.getMessage());
  //    } else if (context instanceof SemanticException) {
  //      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
  //          .setMessage(context.getMessage());
  //    }
  //    return visitStatement(loadTsFileStatement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitCreateTimeseries(
  //      final CreateTimeSeriesStatement statement, final Exception context) {
  //    return visitGeneralCreateTimeSeries(statement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitCreateAlignedTimeseries(
  //      final CreateAlignedTimeSeriesStatement statement, final Exception context) {
  //    return visitGeneralCreateTimeSeries(statement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitCreateMultiTimeseries(
  //      final CreateMultiTimeSeriesStatement statement, final Exception context) {
  //    return visitGeneralCreateTimeSeries(statement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitInternalCreateTimeseries(
  //      final InternalCreateTimeSeriesStatement statement, final Exception context) {
  //    return visitGeneralCreateTimeSeries(statement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitInternalCreateMultiTimeSeries(
  //      final InternalCreateMultiTimeSeriesStatement statement, final Exception context) {
  //    return visitGeneralCreateTimeSeries(statement, context);
  //  }
  //
  //  private TSStatus visitGeneralCreateTimeSeries(
  //      final Statement statement, final Exception context) {
  //    if (context instanceof SemanticException) {
  //      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
  //          .setMessage(context.getMessage());
  //    } else if (isAutoCreateConflict(context)) {
  //      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
  //          .setMessage(context.getCause().getMessage());
  //    }
  //    return visitStatement(statement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitActivateTemplate(
  //      final ActivateTemplateStatement activateTemplateStatement, final Exception context) {
  //    return visitGeneralActivateTemplate(activateTemplateStatement, context);
  //  }
  //
  //  @Override
  //  public TSStatus visitBatchActivateTemplate(
  //      final BatchActivateTemplateStatement batchActivateTemplateStatement,
  //      final Exception context) {
  //    return visitGeneralActivateTemplate(batchActivateTemplateStatement, context);
  //  }
  //
  //  // InternalBatchActivateTemplateNode is converted to BatchActivateTemplateStatement
  //  // No need to handle InternalBatchActivateTemplateStatement
  //
  //  private TSStatus visitGeneralActivateTemplate(
  //      final Statement activateTemplateStatement, final Exception context) {
  //    if (context instanceof MetadataException || context instanceof StatementAnalyzeException) {
  //      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
  //          .setMessage(context.getMessage());
  //    } else if (isAutoCreateConflict(context)) {
  //      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
  //          .setMessage(context.getCause().getMessage());
  //    }
  //    return visitStatement(activateTemplateStatement, context);
  //  }
  //
  //  private boolean isAutoCreateConflict(final Exception e) {
  //    return e instanceof RuntimeException
  //        && e.getCause() instanceof IoTDBException
  //        && e.getCause().getMessage().contains("already been created as database");
  //  }
}
