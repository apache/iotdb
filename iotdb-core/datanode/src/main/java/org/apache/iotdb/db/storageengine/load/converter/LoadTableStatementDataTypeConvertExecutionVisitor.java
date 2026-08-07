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
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.db.storageengine.load.converter.LoadTreeStatementDataTypeConvertExecutionVisitor.handleTSStatus;

public class LoadTableStatementDataTypeConvertExecutionVisitor
    implements AstVisitor<Optional<TSStatus>, String> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTableStatementDataTypeConvertExecutionVisitor.class);

  @FunctionalInterface
  public interface StatementExecutor {
    // databaseName can NOT be null
    TSStatus execute(final Statement statement, final String databaseName);
  }

  private final StatementExecutor statementExecutor;

  public LoadTableStatementDataTypeConvertExecutionVisitor(StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  @Override
  public Optional<TSStatus> visitLoadTsFile(
      final LoadTsFile loadTsFileStatement, final String databaseName) {
    if (Objects.isNull(databaseName)) {
      final String errorMsg =
          String.format(
              "Database name is unexpectedly null for LoadTsFileStatement: %s. Skip data type conversion.",
              loadTsFileStatement);
      LOGGER.warn(errorMsg);
      return Optional.of(
          new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode()).setMessage(errorMsg));
    }

    LOGGER.info(StorageEngineMessages.START_DATA_TYPE_CONVERSION_DOT, loadTsFileStatement);

    final boolean isManagedTask = PipeTsFileConversionTaskManager.getCurrentTaskId() != null;
    final TableConversionContext conversionContext =
        isManagedTask
            ? PipeTsFileConversionTaskManager.getOrCreateCurrentContext(TableConversionContext::new)
            : new TableConversionContext();
    boolean shouldReleaseContext = !isManagedTask;

    try {
      final List<File> files = loadTsFileStatement.getTsFiles();
      while (conversionContext.fileIndex < files.size()) {
        if (conversionContext.pendingStatement != null) {
          final TSStatus status =
              executeInsertTabletWithRetry(conversionContext.pendingStatement, databaseName);
          if (!handleTSStatus(status, loadTsFileStatement)) {
            shouldReleaseContext = !isManagedTask || !isTemporaryUnavailable(status);
            return Optional.of(status);
          }
          conversionContext.pendingStatement = null;
        }

        if (conversionContext.parser == null) {
          conversionContext.parser =
              new TsFileInsertionEventTableParser(
                  files.get(conversionContext.fileIndex),
                  new TablePattern(true, null, null),
                  Long.MIN_VALUE,
                  Long.MAX_VALUE,
                  null,
                  null,
                  null,
                  true);
          conversionContext.iterator =
              conversionContext.parser.toTabletInsertionEvents().iterator();
        }

        while (conversionContext.pendingTabletInsertionEvent != null
            || conversionContext.iterator.hasNext()) {
          if (conversionContext.pendingTabletInsertionEvent == null) {
            conversionContext.pendingTabletInsertionEvent = conversionContext.iterator.next();
          }
          final TabletInsertionEvent tabletInsertionEvent =
              conversionContext.pendingTabletInsertionEvent;
          if (!(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
            conversionContext.pendingTabletInsertionEvent = null;
            continue;
          }
          final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
              (PipeRawTabletInsertionEvent) tabletInsertionEvent;
          final LoadConvertedInsertTabletStatement statement =
              new LoadConvertedInsertTabletStatement(
                  PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
                          rawTabletInsertionEvent.convertToTablet(),
                          rawTabletInsertionEvent.isAligned(),
                          databaseName)
                      .constructStatement(),
                  loadTsFileStatement.isConvertOnTypeMismatch());
          conversionContext.pendingStatement = statement;
          conversionContext.pendingTabletInsertionEvent = null;

          final TSStatus status = executeInsertTabletWithRetry(statement, databaseName);
          if (!handleTSStatus(status, loadTsFileStatement)) {
            shouldReleaseContext = !isManagedTask || !isTemporaryUnavailable(status);
            return Optional.of(status);
          }
          conversionContext.pendingStatement = null;
        }

        conversionContext.closeParser();
        conversionContext.fileIndex++;
      }

      shouldReleaseContext = true;
      if (loadTsFileStatement.isDeleteAfterLoad()) {
        deleteSourceFiles(loadTsFileStatement);
      }

      LOGGER.info(
          StorageEngineMessages
              .STORAGE_LOG_DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL_99016326,
          loadTsFileStatement);
      return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (final Exception e) {
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT_5D132E57,
          loadTsFileStatement,
          e);
      final TSStatus status =
          LoadTsFileDataTypeConverter.TABLE_STATEMENT_EXCEPTION_VISITOR.process(
              loadTsFileStatement, e);
      shouldReleaseContext =
          !isManagedTask || !LoadTsFileDataTypeConverter.isMemoryPressureException(e);
      return Optional.of(status);
    } finally {
      if (shouldReleaseContext) {
        if (isManagedTask) {
          PipeTsFileConversionTaskManager.clearCurrentContext();
        } else {
          conversionContext.close();
        }
      }
    }
  }

  private static boolean isTemporaryUnavailable(final TSStatus status) {
    return status != null
        && (status.getCode() == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()
            || status.getCode()
                == TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode());
  }

  private static void deleteSourceFiles(final LoadTsFile statement) {
    statement
        .getTsFiles()
        .forEach(
            tsFile -> {
              org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(tsFile);
              final String tsFilePath = tsFile.getAbsolutePath();
              org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(
                  new File(LoadUtil.getTsFileResourcePath(tsFilePath)));
              org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(
                  new File(LoadUtil.getTsFileModsV1Path(tsFilePath)));
              org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(
                  new File(LoadUtil.getTsFileModsV2Path(tsFilePath)));
            });
  }

  private static final class TableConversionContext implements AutoCloseable {
    private int fileIndex;
    private TsFileInsertionEventTableParser parser;
    private Iterator<TabletInsertionEvent> iterator;
    private TabletInsertionEvent pendingTabletInsertionEvent;
    private LoadConvertedInsertTabletStatement pendingStatement;

    private void closeParser() {
      if (parser != null) {
        parser.close();
        parser = null;
      }
      iterator = null;
    }

    @Override
    public void close() {
      closeParser();
      pendingTabletInsertionEvent = null;
      pendingStatement = null;
    }
  }

  private TSStatus executeInsertTabletWithRetry(
      final LoadConvertedInsertTabletStatement statement, final String databaseName) {
    TSStatus result;
    try {
      result =
          statement.accept(
              LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
              statementExecutor.execute(statement, databaseName));

      // Retry max 5 times if the write process is rejected
      for (int i = 0;
          i < 5
              && result.getCode()
                  == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode();
          i++) {
        Thread.sleep(100L * (i + 1));
        result =
            statement.accept(
                LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
                statementExecutor.execute(statement, databaseName));
      }
    } catch (final Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      result = statement.accept(LoadTsFileDataTypeConverter.TREE_STATEMENT_EXCEPTION_VISITOR, e);
    }
    return result;
  }
}
