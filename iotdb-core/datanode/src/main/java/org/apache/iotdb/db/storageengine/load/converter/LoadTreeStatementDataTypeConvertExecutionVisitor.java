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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
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

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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
  private final Function<File, LoadTreeTsFileTabletIterator> tabletIteratorFactory;

  @FunctionalInterface
  public interface StatementExecutor {
    TSStatus execute(final Statement statement);
  }

  public LoadTreeStatementDataTypeConvertExecutionVisitor(
      final StatementExecutor statementExecutor) {
    this(statementExecutor, file -> new LoadTreeTsFileTabletIterator(file, true));
  }

  LoadTreeStatementDataTypeConvertExecutionVisitor(
      final StatementExecutor statementExecutor,
      final Function<File, LoadTreeTsFileTabletIterator> tabletIteratorFactory) {
    this.statementExecutor = statementExecutor;
    this.tabletIteratorFactory = tabletIteratorFactory;
  }

  @Override
  public Optional<TSStatus> visitNode(final StatementNode statementNode, final Void v) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Void v) {

    LOGGER.info(StorageEngineMessages.START_DATA_TYPE_CONVERSION, loadTsFileStatement);

    final boolean isManagedTask = PipeTsFileConversionTaskManager.getCurrentTaskId() != null;
    final TreeConversionContext conversionContext =
        isManagedTask
            ? PipeTsFileConversionTaskManager.getOrCreateCurrentContext(TreeConversionContext::new)
            : new TreeConversionContext();
    boolean shouldReleaseContext = !isManagedTask;

    try {
      if (conversionContext.deferredStatus != null) {
        final TSStatus result =
            flushPendingTablets(conversionContext, loadTsFileStatement.isConvertOnTypeMismatch());
        if (!handleTSStatus(result, loadTsFileStatement)) {
          shouldReleaseContext = !isManagedTask || !isTemporaryUnavailable(result);
          return Optional.of(result);
        }
        final TSStatus deferredStatus = conversionContext.deferredStatus;
        conversionContext.deferredStatus = null;
        shouldReleaseContext = true;
        return Optional.of(deferredStatus);
      }

      final List<File> files = loadTsFileStatement.getTsFiles();
      while (conversionContext.fileIndex < files.size()) {
        if (conversionContext.tabletIterator == null) {
          conversionContext.tabletIterator =
              tabletIteratorFactory.apply(files.get(conversionContext.fileIndex));
        }

        if (conversionContext.deferredTabletRawReq != null) {
          final TSStatus result =
              flushPendingTablets(conversionContext, loadTsFileStatement.isConvertOnTypeMismatch());
          if (!handleTSStatus(result, loadTsFileStatement)) {
            shouldReleaseContext = !isManagedTask || !isTemporaryUnavailable(result);
            return Optional.of(result);
          }
          conversionContext.addDeferredTablet();
        }

        while (conversionContext.deferredTabletWithIsAligned != null
            || conversionContext.tabletIterator.hasNext()) {
          if (conversionContext.deferredTabletWithIsAligned == null) {
            conversionContext.deferredTabletWithIsAligned = conversionContext.tabletIterator.next();
          }
          final Pair<Tablet, Boolean> tabletWithIsAligned =
              conversionContext.deferredTabletWithIsAligned;
          final PipeTransferTabletRawReq tabletRawReq =
              PipeTransferTabletRawReq.toTPipeTransferRawReq(
                  tabletWithIsAligned.getLeft(), tabletWithIsAligned.getRight());
          final long currentMemory = calculateTabletSizeInBytes(tabletWithIsAligned.getLeft()) + 1;
          if (conversionContext.block.hasEnoughMemory(currentMemory)) {
            conversionContext.addTablet(tabletRawReq, currentMemory);
            conversionContext.deferredTabletWithIsAligned = null;
            continue;
          }

          final TSStatus result =
              flushPendingTablets(conversionContext, loadTsFileStatement.isConvertOnTypeMismatch());
          if (!handleTSStatus(result, loadTsFileStatement)) {
            conversionContext.deferredTabletRawReq = tabletRawReq;
            conversionContext.deferredTabletRawReqSize = currentMemory;
            conversionContext.deferredTabletWithIsAligned = null;
            shouldReleaseContext = !isManagedTask || !isTemporaryUnavailable(result);
            return Optional.of(result);
          }
          conversionContext.addTablet(tabletRawReq, currentMemory);
          conversionContext.deferredTabletWithIsAligned = null;
        }

        conversionContext.tabletIterator.close();
        conversionContext.tabletIterator = null;
        conversionContext.fileIndex++;
      }

      if (!conversionContext.tabletRawReqs.isEmpty()) {
        final TSStatus result =
            flushPendingTablets(conversionContext, loadTsFileStatement.isConvertOnTypeMismatch());
        if (!handleTSStatus(result, loadTsFileStatement)) {
          shouldReleaseContext = !isManagedTask || !isTemporaryUnavailable(result);
          return Optional.of(result);
        }
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
      final boolean retryable = isRetryableConversionException(e);
      final TSStatus status =
          loadTsFileStatement.accept(
              LoadTsFileDataTypeConverter.TREE_STATEMENT_EXCEPTION_VISITOR, e);

      // A parser can fail after producing tablets that are still waiting for the next batch
      // boundary. Submit those tablets before reporting the parser error so the error does not
      // discard successfully converted data.
      if (!retryable && !conversionContext.tabletRawReqs.isEmpty()) {
        final TSStatus flushStatus =
            flushPendingTablets(conversionContext, loadTsFileStatement.isConvertOnTypeMismatch());
        if (!handleTSStatus(flushStatus, loadTsFileStatement)) {
          if (isManagedTask && isTemporaryUnavailable(flushStatus)) {
            conversionContext.deferredStatus = status;
            shouldReleaseContext = false;
          } else {
            shouldReleaseContext = true;
          }
          return Optional.of(flushStatus);
        }
      }

      shouldReleaseContext = !isManagedTask || !retryable;
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

  private TSStatus flushPendingTablets(
      final TreeConversionContext context, final boolean isConvertedOnTypeMismatch) {
    if (context.tabletRawReqs.isEmpty()) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    final TSStatus result =
        executeInsertMultiTabletsWithRetry(context.tabletRawReqs, isConvertedOnTypeMismatch);
    if (handleTSStatus(result, context)) {
      context.clearPendingTablets();
    }
    return result;
  }

  private static boolean isTemporaryUnavailable(final TSStatus status) {
    return status != null
        && (status.getCode() == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()
            || status.getCode()
                == TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode());
  }

  private static boolean isRetryableConversionException(final Throwable throwable) {
    if (LoadTsFileDataTypeConverter.isMemoryPressureException(throwable)) {
      return true;
    }

    Throwable current = throwable;
    while (current != null) {
      if (current instanceof InterruptedException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static void deleteSourceFiles(final LoadTsFileStatement statement) {
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

  private static final class TreeConversionContext implements AutoCloseable {
    private final LoadTsFileMemoryBlock block =
        LoadTsFileMemoryManager.getInstance()
            .allocateMemoryBlock(TABLET_BATCH_MEMORY_SIZE_IN_BYTES);
    private final List<PipeTransferTabletRawReq> tabletRawReqs = new ArrayList<>();
    private final List<Long> tabletRawReqSizes = new ArrayList<>();
    private int fileIndex;
    private LoadTreeTsFileTabletIterator tabletIterator;
    private Pair<Tablet, Boolean> deferredTabletWithIsAligned;
    private PipeTransferTabletRawReq deferredTabletRawReq;
    private long deferredTabletRawReqSize;
    private TSStatus deferredStatus;

    private void addTablet(final PipeTransferTabletRawReq request, final long size) {
      tabletRawReqs.add(request);
      tabletRawReqSizes.add(size);
      block.addMemoryUsage(size);
    }

    private void addDeferredTablet() {
      addTablet(deferredTabletRawReq, deferredTabletRawReqSize);
      deferredTabletRawReq = null;
      deferredTabletRawReqSize = 0;
    }

    private void clearPendingTablets() {
      for (final long memoryCost : tabletRawReqSizes) {
        block.reduceMemoryUsage(memoryCost);
      }
      tabletRawReqs.clear();
      tabletRawReqSizes.clear();
    }

    @Override
    public void close() {
      if (tabletIterator != null) {
        tabletIterator.close();
        tabletIterator = null;
      }
      clearPendingTablets();
      deferredTabletWithIsAligned = null;
      deferredTabletRawReq = null;
      deferredTabletRawReqSize = 0;
      deferredStatus = null;
      block.close();
    }
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
          StorageEngineMessages
              .STORAGE_LOG_FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT_STATUS_F0311707,
          loadTsFileStatement,
          result.getCode());
      return false;
    }
    return true;
  }
}
