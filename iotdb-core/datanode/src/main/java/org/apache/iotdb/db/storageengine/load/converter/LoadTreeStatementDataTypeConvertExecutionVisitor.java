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
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil.calculateTabletSizeInBytes;

public class LoadTreeStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, Void> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTreeStatementDataTypeConvertExecutionVisitor.class);

  private static final long TABLET_BATCH_MEMORY_SIZE_IN_BYTES =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getLoadTsFileTabletConversionBatchMemorySizeInBytes();

  private final StatementExecutor statementExecutor;

  @FunctionalInterface
  public interface StatementExecutor {
    TSStatus execute(final Statement statement);
  }

  public LoadTreeStatementDataTypeConvertExecutionVisitor(
      final StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  @Override
  public Optional<TSStatus> visitNode(final StatementNode statementNode, final Void v) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Void v) {

    LOGGER.info("Start data type conversion for LoadTsFileStatement: {}", loadTsFileStatement);

    final LoadTsFileMemoryBlock block =
        LoadTsFileMemoryManager.getInstance()
            .allocateMemoryBlock(TABLET_BATCH_MEMORY_SIZE_IN_BYTES);
    final List<PipeTransferTabletRawReq> tabletRawReqs = new ArrayList<>();
    final List<Long> tabletRawReqSizes = new ArrayList<>();

    try {
      for (final File file : loadTsFileStatement.getTsFiles()) {
        try (final TsFileInsertionEventScanParser parser =
            new TsFileInsertionEventScanParser(
                file,
                new IoTDBTreePattern(null),
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                null,
                null,
                true)) {
          for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
            final PipeTransferTabletRawReq tabletRawReq =
                PipeTransferTabletRawReq.toTPipeTransferRawReq(
                    tabletWithIsAligned.getLeft(), tabletWithIsAligned.getRight());
            final long curMemory = calculateTabletSizeInBytes(tabletWithIsAligned.getLeft()) + 1;
            if (block.hasEnoughMemory(curMemory)) {
              tabletRawReqs.add(tabletRawReq);
              tabletRawReqSizes.add(curMemory);
              block.addMemoryUsage(curMemory);
              continue;
            }

            final TSStatus result =
                executeInsertMultiTabletsWithRetry(
                    tabletRawReqs, loadTsFileStatement.isConvertOnTypeMismatch());

            for (final long memoryCost : tabletRawReqSizes) {
              block.reduceMemoryUsage(memoryCost);
            }
            tabletRawReqs.clear();
            tabletRawReqSizes.clear();

            if (!handleTSStatus(result, loadTsFileStatement)) {
              return Optional.of(result);
            }

            tabletRawReqs.add(tabletRawReq);
            tabletRawReqSizes.add(curMemory);
            block.addMemoryUsage(curMemory);
          }
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
          return Optional.of(
              loadTsFileStatement.accept(
                  LoadTsFileDataTypeConverter.TREE_STATEMENT_EXCEPTION_VISITOR, e));
        }
      }

      if (!tabletRawReqs.isEmpty()) {
        try {
          final TSStatus result =
              executeInsertMultiTabletsWithRetry(
                  tabletRawReqs, loadTsFileStatement.isConvertOnTypeMismatch());

          for (final long memoryCost : tabletRawReqSizes) {
            block.reduceMemoryUsage(memoryCost);
          }
          tabletRawReqs.clear();
          tabletRawReqSizes.clear();

          if (!handleTSStatus(result, loadTsFileStatement)) {
            return Optional.of(result);
          }
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
          return Optional.of(
              loadTsFileStatement.accept(
                  LoadTsFileDataTypeConverter.TREE_STATEMENT_EXCEPTION_VISITOR, e));
        }
      }
    } finally {
      for (final long memoryCost : tabletRawReqSizes) {
        block.reduceMemoryUsage(memoryCost);
      }
      tabletRawReqs.clear();
      tabletRawReqSizes.clear();
      block.close();
    }

    if (loadTsFileStatement.isDeleteAfterLoad()) {
      loadTsFileStatement
          .getTsFiles()
          .forEach(
              tsfile -> {
                FileUtils.deleteQuietly(tsfile);
                final String tsFilePath = tsfile.getAbsolutePath();
                FileUtils.deleteQuietly(new File(LoadUtil.getTsFileResourcePath(tsFilePath)));
                FileUtils.deleteQuietly(new File(LoadUtil.getTsFileModsV1Path(tsFilePath)));
                FileUtils.deleteQuietly(new File(LoadUtil.getTsFileModsV2Path(tsFilePath)));
              });
    }

    LOGGER.info(
        "Data type conversion for LoadTsFileStatement {} is successful.", loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private TSStatus executeInsertMultiTabletsWithRetry(
      final List<PipeTransferTabletRawReq> tabletRawReqs, final boolean isConvertedOnTypeMismatch) {
    final InsertMultiTabletsStatement batchStatement = new InsertMultiTabletsStatement();
    batchStatement.setInsertTabletStatementList(
        tabletRawReqs.stream()
            .map(
                req ->
                    new LoadConvertedInsertTabletStatement(
                        req.constructStatement(), isConvertedOnTypeMismatch))
            .collect(Collectors.toList()));

    TSStatus result;
    try {
      result =
          batchStatement.accept(
              LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
              statementExecutor.execute(batchStatement));

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
                statementExecutor.execute(batchStatement));
      }
    } catch (final Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      result =
          batchStatement.accept(LoadTsFileDataTypeConverter.TREE_STATEMENT_EXCEPTION_VISITOR, e);
    }
    return result;
  }

  public static boolean handleTSStatus(final TSStatus result, final Object loadTsFileStatement) {
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
