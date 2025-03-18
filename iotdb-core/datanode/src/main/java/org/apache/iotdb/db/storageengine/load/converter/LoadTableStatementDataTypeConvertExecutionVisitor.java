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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil.calculateTabletSizeInBytes;

public class LoadTableStatementDataTypeConvertExecutionVisitor
    extends AstVisitor<Optional<TSStatus>, String> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTableStatementDataTypeConvertExecutionVisitor.class);

  private static final long TABLET_BATCH_MEMORY_SIZE_IN_BYTES =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getLoadTsFileTabletConversionBatchMemorySizeInBytes();

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
      LOGGER.warn(
          "Database name is unexpectedly null for LoadTsFileStatement: {}. Skip data type conversion.",
          loadTsFileStatement);
      return Optional.empty();
    }

    LOGGER.info("Start data type conversion for LoadTsFileStatement: {}.", loadTsFileStatement);

    final LoadTsFileMemoryBlock block =
        LoadTsFileMemoryManager.getInstance()
            .allocateMemoryBlock(TABLET_BATCH_MEMORY_SIZE_IN_BYTES);
    final List<PipeTransferTabletRawReqV2> tabletRawReqs = new ArrayList<>();

    for (final File file : loadTsFileStatement.getTsFiles()) {
      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              file,
              new TablePattern(true, null, null),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              "root",
              null)) {
        for (final TabletInsertionEvent tabletInsertionEvent : parser.toTabletInsertionEvents()) {
          if (!(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
            continue;
          }
          final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
              (PipeRawTabletInsertionEvent) tabletInsertionEvent;

          final Tablet tablet = rawTabletInsertionEvent.convertToTablet();
          final PipeTransferTabletRawReqV2 tabletRawReq =
              PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
                  tablet, rawTabletInsertionEvent.isAligned(), databaseName);
          tabletRawReqs.add(tabletRawReq);
          block.addMemoryUsage(calculateTabletSizeInBytes(tablet) + 1);
          if (block.hasEnoughMemory()) {
            continue;
          }

          final TSStatus result =
              executeInsertMultiTabletsWithRetry(
                  tabletRawReqs, databaseName, loadTsFileStatement.isConvertOnTypeMismatch());

          tabletRawReqs.clear();
          block.clearMemoryUsage();

          if (!handleTSStatus(result, loadTsFileStatement)) {
            return Optional.empty();
          }
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
        return Optional.empty();
      }
    }

    if (!tabletRawReqs.isEmpty()) {
      try {
        final TSStatus result =
            executeInsertMultiTabletsWithRetry(
                tabletRawReqs, databaseName, loadTsFileStatement.isConvertOnTypeMismatch());

        tabletRawReqs.clear();
        block.clearMemoryUsage();

        if (!handleTSStatus(result, loadTsFileStatement)) {
          return Optional.empty();
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
        return Optional.empty();
      }
    }

    block.close();

    if (loadTsFileStatement.isDeleteAfterLoad()) {
      loadTsFileStatement.getTsFiles().forEach(FileUtils::deleteQuietly);
    }

    LOGGER.info(
        "Data type conversion for LoadTsFileStatement {} is successful.", loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private TSStatus executeInsertMultiTabletsWithRetry(
      final List<PipeTransferTabletRawReqV2> tabletRawReqs,
      String databaseName,
      boolean isConvertOnTypeMismatch) {
    final InsertMultiTabletsStatement batchStatement = new InsertMultiTabletsStatement();
    batchStatement.setInsertTabletStatementList(
        tabletRawReqs.stream()
            .map(
                req ->
                    new LoadConvertedInsertTabletStatement(
                        req.constructStatement(), isConvertOnTypeMismatch))
            .collect(Collectors.toList()));

    TSStatus result;
    try {
      result =
          batchStatement.accept(
              LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
              statementExecutor.execute(batchStatement, databaseName));

      // Retry max 5 times if the write process is rejected
      for (int i = 0;
          i < 5
              && result.getCode()
                  == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode();
          i++) {
        Thread.sleep(100L * (i + 1));
        result =
            batchStatement.accept(
                LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
                statementExecutor.execute(batchStatement, databaseName));
      }
    } catch (final Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      result = batchStatement.accept(LoadTsFileDataTypeConverter.STATEMENT_EXCEPTION_VISITOR, e);
    }
    return result;
  }

  private static boolean handleTSStatus(
      final TSStatus result, final LoadTsFile loadTsFileStatement) {
    if (!(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
        || result.getCode() == TSStatusCode.LOAD_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())) {
      LOGGER.warn(
          "Failed to convert data type for LoadTsFileStatement: {}, status code is {}.",
          loadTsFileStatement,
          result.getCode());
      return false;
    }
    return true;
  }
}
