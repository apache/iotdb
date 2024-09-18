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

/**
 * This visitor translated some {@link TSStatus} to pipe related status to help sender classify them
 * and apply different error handling tactics. Please DO NOT modify the {@link TSStatus} returned by
 * the processes that generate the following {@link TSStatus}es in the class.
 */
public class PipeStatementTSStatusVisitor extends StatementVisitor<TSStatus, TSStatus> {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public TSStatus visitNode(final StatementNode node, final TSStatus context) {
    return context;
  }

  @Override
  public TSStatus visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()
        || context.getCode() == TSStatusCode.LOAD_FILE_ERROR.getStatusCode()
            && context.getMessage() != null
            && context.getMessage().contains("memory")) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitLoadFile(loadTsFileStatement, context);
  }

  @Override
  public TSStatus visitInsertTablet(
      final InsertTabletStatement insertTabletStatement, final TSStatus context) {
    return visitInsertBase(insertTabletStatement, context);
  }

  @Override
  public TSStatus visitInsertRow(
      final InsertRowStatement insertRowStatement, final TSStatus context) {
    return visitInsertBase(insertRowStatement, context);
  }

  @Override
  public TSStatus visitInsertRows(
      final InsertRowsStatement insertRowsStatement, final TSStatus context) {
    return visitInsertBase(insertRowsStatement, context);
  }

  @Override
  public TSStatus visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement, final TSStatus context) {
    return visitInsertBase(insertMultiTabletsStatement, context);
  }

  private TSStatus visitInsertBase(
      final InsertBaseStatement insertBaseStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.OUT_OF_TTL.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
        && (context.getMessage().contains(DataTypeMismatchException.REGISTERED_TYPE_STRING)
            && config.isEnablePartialInsert())) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(insertBaseStatement, context);
  }

  @Override
  public TSStatus visitCreateTimeseries(
      final CreateTimeSeriesStatement statement, final TSStatus context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  @Override
  public TSStatus visitCreateAlignedTimeseries(
      final CreateAlignedTimeSeriesStatement statement, final TSStatus context) {
    return visitGeneralCreateTimeSeries(statement, context);
  }

  private TSStatus visitGeneralCreateTimeSeries(final Statement statement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.ALIAS_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.PATH_ALREADY_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.ALIGNED_TIMESERIES_ERROR.getStatusCode()
        || context.getCode() == TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(statement, context);
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, final TSStatus context) {
    return visitGeneralCreateMultiTimeseries(createMultiTimeSeriesStatement, context);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      final InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement,
      final TSStatus context) {
    return visitGeneralCreateMultiTimeseries(internalCreateTimeSeriesStatement, context);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement,
      final TSStatus context) {
    return visitGeneralCreateMultiTimeseries(internalCreateMultiTimeSeriesStatement, context);
  }

  private TSStatus visitGeneralCreateMultiTimeseries(
      final Statement statement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      for (final TSStatus status : context.getSubStatus()) {
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()
            && status.getCode() != TSStatusCode.ALIAS_ALREADY_EXIST.getStatusCode()) {
          if (status.getCode() == TSStatusCode.ALIGNED_TIMESERIES_ERROR.getStatusCode()) {
            return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(context.getMessage());
          }
          return visitStatement(statement, context);
        }
      }
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(statement, context);
  }

  @Override
  public TSStatus visitAlterTimeSeries(
      final AlterTimeSeriesStatement alterTimeSeriesStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      if (context.getMessage().contains("already")) {
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      } else if (context.getMessage().contains("does not")) {
        return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      }
    } else if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(alterTimeSeriesStatement, context);
  }

  @Override
  public TSStatus visitCreateLogicalView(
      final CreateLogicalViewStatement createLogicalViewStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      for (final TSStatus status : context.getSubStatus()) {
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
          return visitStatement(createLogicalViewStatement, context);
        }
      }
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateLogicalView(createLogicalViewStatement, context);
  }

  @Override
  public TSStatus visitActivateTemplate(
      final ActivateTemplateStatement activateTemplateStatement, final TSStatus context) {
    return visitGeneralActivateTemplate(activateTemplateStatement, context);
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      final BatchActivateTemplateStatement batchActivateTemplateStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      for (final TSStatus status : context.getSubStatus()) {
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
          return visitStatement(batchActivateTemplateStatement, context);
        }
      }
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitGeneralActivateTemplate(batchActivateTemplateStatement, context);
  }

  private TSStatus visitGeneralActivateTemplate(
      final Statement activateTemplateStatement, final TSStatus context) {
    if (context.getCode() == TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitStatement(activateTemplateStatement, context);
  }
}
