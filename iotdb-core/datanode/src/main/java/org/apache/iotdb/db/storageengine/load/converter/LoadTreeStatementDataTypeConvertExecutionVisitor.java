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
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

  private static final AtomicReference<WrappedThreadPoolExecutor> executorPool =
      new AtomicReference<>();

  private final StatementExecutor statementExecutor;

  @FunctionalInterface
  public interface StatementExecutor {
    TSStatus execute(final Statement statement);
  }

  public static class CallerBlocksPolicy implements RejectedExecutionHandler {
    public CallerBlocksPolicy() {}

    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
      if (!e.isShutdown()) {
        try {
          e.getQueue().put(r);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RejectedExecutionException("task " + r + " rejected from " + e, ie);
        }
      }
    }
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
            .allocateMemoryBlock(
                TABLET_BATCH_MEMORY_SIZE_IN_BYTES
                    * IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getLoadTsFileTabletConversionThreadCount());
    final List<PipeTransferTabletRawReq> tabletRawReqs = new ArrayList<>();
    final List<Long> tabletRawReqSizes = new ArrayList<>();

    try {
      final List<Future<TSStatus>> executionFutures = new ArrayList<>();
      for (final File file : loadTsFileStatement.getTsFiles()) {
        try (final TsFileInsertionEventScanParser parser =
            new TsFileInsertionEventScanParser(
                file, new IoTDBTreePattern(null), Long.MIN_VALUE, Long.MAX_VALUE, null, null)) {
          for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
            final PipeTransferTabletRawReq tabletRawReq =
                PipeTransferTabletRawReq.toTPipeTransferRawReq(
                    tabletWithIsAligned.getLeft(), tabletWithIsAligned.getRight());
            final long curMemory = calculateTabletSizeInBytes(tabletWithIsAligned.getLeft()) + 1;
            if (block.hasEnoughMemory(
                curMemory
                    + TABLET_BATCH_MEMORY_SIZE_IN_BYTES
                        * Math.max(
                            0,
                            (IoTDBDescriptor.getInstance()
                                    .getConfig()
                                    .getLoadTsFileTabletConversionThreadCount()
                                - 1)))) {
              tabletRawReqs.add(tabletRawReq);
              tabletRawReqSizes.add(curMemory);
              block.addMemoryUsage(curMemory);
              continue;
            }

            final InsertMultiTabletsStatement batchStatement = new InsertMultiTabletsStatement();
            batchStatement.setInsertTabletStatementList(
                tabletRawReqs.stream()
                    .map(
                        req ->
                            new LoadConvertedInsertTabletStatement(
                                req.constructStatement(),
                                loadTsFileStatement.isConvertOnTypeMismatch()))
                    .collect(Collectors.toList()));
            executionFutures.add(executeInsertMultiTabletsWithRetry(batchStatement));

            for (final long memoryCost : tabletRawReqSizes) {
              block.reduceMemoryUsage(memoryCost);
            }
            tabletRawReqs.clear();
            tabletRawReqSizes.clear();

            tabletRawReqs.add(tabletRawReq);
            tabletRawReqSizes.add(curMemory);
            block.addMemoryUsage(curMemory);
          }
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
          return Optional.empty();
        }
      }

      if (!tabletRawReqs.isEmpty()) {
        try {
          final InsertMultiTabletsStatement batchStatement = new InsertMultiTabletsStatement();
          batchStatement.setInsertTabletStatementList(
              tabletRawReqs.stream()
                  .map(
                      req ->
                          new LoadConvertedInsertTabletStatement(
                              req.constructStatement(),
                              loadTsFileStatement.isConvertOnTypeMismatch()))
                  .collect(Collectors.toList()));
          executionFutures.add(executeInsertMultiTabletsWithRetry(batchStatement));

          for (final long memoryCost : tabletRawReqSizes) {
            block.reduceMemoryUsage(memoryCost);
          }
          tabletRawReqs.clear();
          tabletRawReqSizes.clear();
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
          return Optional.empty();
        }
      }

      for (final Future<TSStatus> future : executionFutures) {
        try {
          if (!handleTSStatus(future.get(), loadTsFileStatement)) {
            return Optional.empty();
          }
        } catch (ExecutionException | InterruptedException e) {
          LOGGER.warn("Exception occurs when executing insertion during tablet conversion: ", e);
          return Optional.empty();
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
                FileUtils.deleteQuietly(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX));
                FileUtils.deleteQuietly(new File(tsFilePath + ModificationFileV1.FILE_SUFFIX));
                FileUtils.deleteQuietly(new File(tsFilePath + ModificationFile.FILE_SUFFIX));
              });
    }

    LOGGER.info(
        "Data type conversion for LoadTsFileStatement {} is successful.", loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private Future<TSStatus> executeInsertMultiTabletsWithRetry(
      final InsertMultiTabletsStatement batchStatement) {
    return getExecutorPool()
        .submit(
            () -> {
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
                    batchStatement.accept(
                        LoadTsFileDataTypeConverter.STATEMENT_EXCEPTION_VISITOR, e);
              }
              return result;
            });
  }

  public static WrappedThreadPoolExecutor getExecutorPool() {
    if (executorPool.get() == null) {
      synchronized (executorPool) {
        if (executorPool.get() == null) {
          executorPool.set(
              new WrappedThreadPoolExecutor(
                  IoTDBDescriptor.getInstance()
                      .getConfig()
                      .getLoadTsFileTabletConversionThreadCount(),
                  IoTDBDescriptor.getInstance()
                      .getConfig()
                      .getLoadTsFileTabletConversionThreadCount(),
                  0L,
                  TimeUnit.SECONDS,
                  new ArrayBlockingQueue<>(
                      IoTDBDescriptor.getInstance()
                          .getConfig()
                          .getLoadTsFileTabletConversionThreadCount()),
                  new IoTThreadFactory(ThreadName.LOAD_DATATYPE_CONVERT_POOL.getName()),
                  ThreadName.LOAD_DATATYPE_CONVERT_POOL.getName(),
                  new CallerBlocksPolicy()));
        }
      }
    }
    return executorPool.get();
  }

  private static boolean handleTSStatus(
      final TSStatus result, final LoadTsFileStatement loadTsFileStatement) {
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
