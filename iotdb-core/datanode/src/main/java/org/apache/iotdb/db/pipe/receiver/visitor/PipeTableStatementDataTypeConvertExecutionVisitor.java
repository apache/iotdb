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
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.db.pipe.receiver.transform.statement.PipeConvertedInsertRowStatement;
import org.apache.iotdb.db.pipe.receiver.transform.statement.PipeConvertedInsertTabletStatement;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.LoadTsFileManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This visitor transforms the data type of the statement when the statement is executed and an
 * exception occurs. The transformed statement (if any) is returned and will be executed again.
 */
public class PipeTableStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, Pair<TSStatus, String>> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTableStatementDataTypeConvertExecutionVisitor.class);

  @FunctionalInterface
  public interface StatementExecutor {
    // databaseName can NOT be null
    TSStatus execute(final Statement statement, final String databaseName);
  }

  private final StatementExecutor statementExecutor;

  public PipeTableStatementDataTypeConvertExecutionVisitor(
      final StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  private Optional<TSStatus> tryExecute(final Statement statement, final String databaseName) {
    try {
      if (Objects.isNull(databaseName)) {
        if (LOGGER.isWarnEnabled()) {
          PipeLogger.log(
              LOGGER::warn,
              DataNodePipeMessages.DATABASE_NAME_IS_UNEXPECTEDLY_NULL_SKIP_DATA_TYPE_CONVERSION);
        }
        return Optional.empty();
      }

      return Optional.of(statementExecutor.execute(statement, databaseName));
    } catch (final Exception e) {
      if (LOGGER.isWarnEnabled()) {
        PipeLogger.log(
            LOGGER::warn,
            DataNodePipeMessages
                .FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE_CONVERSION_WITH_EXCEPTION_TYPE,
            e.getClass().getName());
      }
      return Optional.empty();
    }
  }

  @Override
  public Optional<TSStatus> visitNode(
      final StatementNode statementNode, final Pair<TSStatus, String> statusAndDatabaseName) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement,
      final Pair<TSStatus, String> statusAndDatabaseName) {
    final TSStatus status = statusAndDatabaseName.getLeft();
    final String databaseName = statusAndDatabaseName.getRight();

    if (Objects.isNull(databaseName)) {
      LOGGER.warn(
          DataNodePipeMessages.DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_LOADTSFILESTATEMENT,
          loadTsFileStatement);
      return Optional.empty();
    }

    if (!PipeConfig.getInstance().isPipeReceiverLoadConversionEnabled()) {
      return Optional.empty();
    }

    if (status.getCode() != TSStatusCode.LOAD_FILE_ERROR.getStatusCode()
        // Ignore the error if it is caused by insufficient memory
        || (status.getMessage() != null && status.getMessage().contains("memory"))) {
      return Optional.empty();
    }

    LOGGER.warn(
        DataNodePipeMessages.DATA_TYPE_MISMATCH_DETECTED_TSSTATUS_FOR_LOADTSFILESTATEMENT,
        status,
        loadTsFileStatement);

    for (final File file : loadTsFileStatement.getTsFiles()) {
      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              file,
              new TablePattern(true, null, null),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              null,
              true)) {
        for (final TabletInsertionEvent tabletInsertionEvent : parser.toTabletInsertionEvents()) {
          if (!(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
            continue;
          }
          final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
              (PipeRawTabletInsertionEvent) tabletInsertionEvent;

          final PipeConvertedInsertTabletStatement statement =
              new PipeConvertedInsertTabletStatement(
                  PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
                          rawTabletInsertionEvent.convertToTablet(),
                          rawTabletInsertionEvent.isAligned(),
                          databaseName)
                      .constructStatement(),
                  false);

          TSStatus result;
          try {
            result =
                IoTDBDataNodeReceiver.STATEMENT_STATUS_VISITOR.process(
                    statement, statementExecutor.execute(statement, databaseName));

            // Retry max 5 times if the write process is rejected
            for (int i = 0;
                i < 5
                    && result.getCode()
                        == TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
                            .getStatusCode();
                i++) {
              Thread.sleep(100L * (i + 1));
              result =
                  IoTDBDataNodeReceiver.STATEMENT_STATUS_VISITOR.process(
                      statement, statementExecutor.execute(statement, databaseName));
            }
          } catch (final Exception e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            result = IoTDBDataNodeReceiver.STATEMENT_EXCEPTION_VISITOR.process(statement, e);
          }

          if (!(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
              || result.getCode()
                  == TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())) {
            return Optional.empty();
          }
        }
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages.FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT,
            loadTsFileStatement,
            e);
        return Optional.empty();
      }
    }

    if (loadTsFileStatement.isDeleteAfterLoad()) {
      loadTsFileStatement.getTsFiles().forEach(LoadTsFileManager::cleanTsFile);
    }

    LOGGER.warn(
        DataNodePipeMessages.DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL,
        loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  @Override
  public Optional<TSStatus> visitInsertRow(
      final InsertRowStatement insertRowStatement,
      final Pair<TSStatus, String> statusAndDatabaseName) {
    return tryExecute(
        new PipeConvertedInsertRowStatement(insertRowStatement), statusAndDatabaseName.getRight());
  }

  @Override
  public Optional<TSStatus> visitInsertRows(
      final InsertRowsStatement insertRowsStatement,
      final Pair<TSStatus, String> statusAndDatabaseName) {
    if (insertRowsStatement.getInsertRowStatementList() == null
        || insertRowsStatement.getInsertRowStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertRowsStatement convertedInsertRowsStatement = new InsertRowsStatement();
    convertedInsertRowsStatement.setInsertRowStatementList(
        insertRowsStatement.getInsertRowStatementList().stream()
            .map(PipeConvertedInsertRowStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertRowsStatement, statusAndDatabaseName.getRight());
  }

  @Override
  public Optional<TSStatus> visitInsertRowsOfOneDevice(
      final InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement,
      final Pair<TSStatus, String> statusAndDatabaseName) {
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
    return tryExecute(convertedInsertRowsOfOneDeviceStatement, statusAndDatabaseName.getRight());
  }

  @Override
  public Optional<TSStatus> visitInsertTablet(
      final InsertTabletStatement insertTabletStatement,
      final Pair<TSStatus, String> statusAndDatabaseName) {
    return tryExecute(
        new PipeConvertedInsertTabletStatement(insertTabletStatement),
        statusAndDatabaseName.getRight());
  }

  @Override
  public Optional<TSStatus> visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement,
      final Pair<TSStatus, String> statusAndDatabaseName) {
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
    return tryExecute(convertedInsertMultiTabletsStatement, statusAndDatabaseName.getRight());
  }
}
