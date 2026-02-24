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
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.stream.Collectors;

/**
 * This visitor translated some {@link TSStatus} to pipe related status to help sender classify them
 * and apply different error handling tactics. Please DO NOT modify the {@link TSStatus} returned by
 * the processes that generate the following {@link TSStatus}es in the class.
 */
public class PipeStatementTSStatusVisitor extends StatementVisitor<TSStatus, TSStatus> {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public TSStatus process(final StatementNode node, final TSStatus status) {
    return status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()
        ? PipeReceiverStatusHandler.getPriorStatus(
            status.getSubStatus().stream()
                .map(subStatus -> node.accept(this, subStatus))
                .collect(Collectors.toList()))
        : node.accept(this, status);
  }

  @Override
  public TSStatus visitNode(final StatementNode node, final TSStatus status) {
    return status;
  }

  @Override
  public TSStatus visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()
        || status.getCode() == TSStatusCode.LOAD_FILE_ERROR.getStatusCode()
            && status.getMessage() != null
            && status.getMessage().contains("memory")) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return super.visitLoadFile(loadTsFileStatement, status);
  }

  @Override
  public TSStatus visitInsertTablet(
      final InsertTabletStatement insertTabletStatement, final TSStatus status) {
    return visitInsertBase(insertTabletStatement, status);
  }

  @Override
  public TSStatus visitInsertRow(
      final InsertRowStatement insertRowStatement, final TSStatus status) {
    return visitInsertBase(insertRowStatement, status);
  }

  @Override
  public TSStatus visitInsertRows(
      final InsertRowsStatement insertRowsStatement, final TSStatus status) {
    return visitInsertBase(insertRowsStatement, status);
  }

  @Override
  public TSStatus visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement, final TSStatus status) {
    return visitInsertBase(insertMultiTabletsStatement, status);
  }

  @Override
  public TSStatus visitInsertBase(
      final InsertBaseStatement insertBaseStatement, final TSStatus status) {
    // If the system is read-only, we shall not classify it into temporary unavailable exception to
    // avoid to many logs
    if (status.getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    } else if (status.getCode() == TSStatusCode.OUT_OF_TTL.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    } else if (status.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    } else if (status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      if (status.getMessage().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING)
          && config.isEnablePartialInsert()) {
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(status.getMessage());
      }
      if (status.getMessage().contains("does not exist")) {
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(status.getMessage());
      }
    }
    return visitStatement(insertBaseStatement, status);
  }

  @Override
  public TSStatus visitCreateTimeseries(
      final CreateTimeSeriesStatement statement, final TSStatus status) {
    return visitGeneralCreateTimeSeries(statement, status);
  }

  @Override
  public TSStatus visitCreateAlignedTimeseries(
      final CreateAlignedTimeSeriesStatement statement, final TSStatus status) {
    return visitGeneralCreateTimeSeries(statement, status);
  }

  private TSStatus visitGeneralCreateTimeSeries(final Statement statement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()
        || status.getCode() == TSStatusCode.ALIAS_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    } else if (status.getCode() == TSStatusCode.PATH_ALREADY_EXIST.getStatusCode()
        || status.getCode() == TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return visitStatement(statement, status);
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, final TSStatus status) {
    return visitGeneralCreateMultiTimeSeries(createMultiTimeSeriesStatement, status);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      final InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement,
      final TSStatus status) {
    return visitGeneralCreateMultiTimeSeries(internalCreateTimeSeriesStatement, status);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement,
      final TSStatus status) {
    return visitGeneralCreateMultiTimeSeries(internalCreateMultiTimeSeriesStatement, status);
  }

  private TSStatus visitGeneralCreateMultiTimeSeries(
      final Statement statement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()
        || status.getCode() == TSStatusCode.ALIAS_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    } else if (status.getCode() == TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return visitStatement(statement, status);
  }

  @Override
  public TSStatus visitAlterTimeSeries(
      final AlterTimeSeriesStatement alterTimeSeriesStatement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      if (status.getMessage().contains("already")) {
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(status.getMessage());
      } else if (status.getMessage().contains("does not")) {
        return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(status.getMessage());
      }
    } else if (status.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return visitStatement(alterTimeSeriesStatement, status);
  }

  @Override
  public TSStatus visitCreateLogicalView(
      final CreateLogicalViewStatement createLogicalViewStatement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return super.visitCreateLogicalView(createLogicalViewStatement, status);
  }

  @Override
  public TSStatus visitActivateTemplate(
      final ActivateTemplateStatement activateTemplateStatement, final TSStatus status) {
    return visitGeneralActivateTemplate(activateTemplateStatement, status);
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      final BatchActivateTemplateStatement batchActivateTemplateStatement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    if (status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
        && status.isSetMessage()
        && status.getMessage().contains("has not been set any template")) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return visitGeneralActivateTemplate(batchActivateTemplateStatement, status);
  }

  private TSStatus visitGeneralActivateTemplate(
      final Statement activateTemplateStatement, final TSStatus status) {
    if (status.getCode() == TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    if (status.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
        && status.isSetMessage()
        && status.getMessage().contains("has not been set any template")) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(status.getMessage());
    }
    return visitStatement(activateTemplateStatement, status);
  }
}
