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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;

/**
 * This visitor translated some exceptions to pipe related {@link TSStatus} to help sender classify
 * them and apply different error handling tactics. Please DO NOT modify the exceptions returned by
 * the processes that generate the following exceptions in the class.
 */
public class PipeStatementExceptionVisitor extends StatementVisitor<TSStatus, Exception> {
  @Override
  public TSStatus visitNode(final StatementNode node, final Exception context) {
    return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
        .setMessage(context.getMessage());
  }

  @Override
  public TSStatus visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Exception context) {
    if (context instanceof LoadRuntimeOutOfMemoryException) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context instanceof SemanticException) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(loadTsFileStatement, context);
  }

  @Override
  public TSStatus visitCreateTimeseries(
      final CreateTimeSeriesStatement statement, final Exception context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  @Override
  public TSStatus visitCreateAlignedTimeseries(
      final CreateAlignedTimeSeriesStatement statement, final Exception context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesStatement statement, final Exception context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      final InternalCreateTimeSeriesStatement statement, final Exception context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesStatement statement, final Exception context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  private TSStatus visitGeneralCreateTimeSeries(
      final Statement statement, final Exception context) {
    if (context instanceof SemanticException) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (isAutoCreateConflict(context)) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getCause().getMessage());
    }
    return visitStatement(statement, context);
  }

  @Override
  public TSStatus visitActivateTemplate(
      final ActivateTemplateStatement activateTemplateStatement, final Exception context) {
    return visitGeneralActivateTemplate(activateTemplateStatement, context);
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      final BatchActivateTemplateStatement batchActivateTemplateStatement,
      final Exception context) {
    return visitGeneralActivateTemplate(batchActivateTemplateStatement, context);
  }

  // InternalBatchActivateTemplateNode is converted to BatchActivateTemplateStatement
  // No need to handle InternalBatchActivateTemplateStatement

  private TSStatus visitGeneralActivateTemplate(
      final Statement activateTemplateStatement, final Exception context) {
    if (context instanceof MetadataException || context instanceof StatementAnalyzeException) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (isAutoCreateConflict(context)) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getCause().getMessage());
    }
    return visitStatement(activateTemplateStatement, context);
  }

  private boolean isAutoCreateConflict(final Exception e) {
    return e instanceof RuntimeException
        && e.getCause() instanceof IoTDBException
        && e.getCause().getMessage().contains("already been created as database");
  }
}
