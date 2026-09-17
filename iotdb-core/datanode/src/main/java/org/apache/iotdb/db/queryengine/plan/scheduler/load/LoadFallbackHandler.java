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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * Failure fallback of the LOAD scheduler: when at least one TsFile fails, this handler converts the
 * failed files into tablets and retries the insertion, then resolves the final result. Extracted
 * from the scheduler so the failure path is a single, testable component.
 *
 * <p>Flow ({@link #convertFailedTsFilesToTablets()}):
 *
 * <ol>
 *   <li>builds the comma-separated list of failed file paths for logging;
 *   <li>for every failed file, converts it with {@link LoadTsFileDataTypeConverter} - table-model
 *       files through {@code convertForTableModel}, tree-model files through {@code
 *       convertForTreeModel} with a retry statement built by {@code buildRetryTreeLoadStatement};
 *   <li>removes successfully converted files from the failure list;
 *   <li>if no failure remains the load is considered successful (FINISHED); otherwise the state
 *       machine transitions to FAILED with the remaining file list.
 * </ol>
 *
 * The whole conversion is measured as the {@code SCHEDULER_CAST_TABLETS} phase metric.
 */
public class LoadFallbackHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadFallbackHandler.class);

  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  private final MPPQueryContext queryContext;
  private final boolean isGeneratedByPipe;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final List<Integer> failedTsFileNodeIndexes;
  private final QueryStateMachine stateMachine;

  public LoadFallbackHandler(
      MPPQueryContext queryContext,
      boolean isGeneratedByPipe,
      List<LoadSingleTsFileNode> tsFileNodeList,
      List<Integer> failedTsFileNodeIndexes,
      QueryStateMachine stateMachine) {
    this.queryContext = queryContext;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.tsFileNodeList = tsFileNodeList;
    this.failedTsFileNodeIndexes = failedTsFileNodeIndexes;
    this.stateMachine = stateMachine;
  }

  public void convertFailedTsFilesToTablets() {
    final StringBuilder failedTsFiles =
        new StringBuilder(
            !tsFileNodeList.isEmpty()
                ? tsFileNodeList
                    .get(failedTsFileNodeIndexes.get(0))
                    .getTsFileResource()
                    .getTsFilePath()
                : "");
    final ListIterator<Integer> iterator = failedTsFileNodeIndexes.listIterator(1);
    while (iterator.hasNext()) {
      failedTsFiles
          .append(", ")
          .append(tsFileNodeList.get(iterator.next()).getTsFileResource().getTsFilePath());
    }

    final long startTime = System.nanoTime();
    try {
      // if failed to load some TsFiles, then try to convert the TsFiles to Tablets
      LOGGER.info(
          DataNodeQueryMessages
              .LOAD_TSFILE_S_FAILED_WILL_TRY_TO_CONVERT_TO_TABLETS_AND_INSERT_FAILED_TSFILES_ARG,
          failedTsFiles);
      convertFailedTsFilesToTabletsAndRetry();
    } finally {
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.SCHEDULER_CAST_TABLETS, System.nanoTime() - startTime);
    }
  }

  private void convertFailedTsFilesToTabletsAndRetry() {
    final LoadTsFileDataTypeConverter loadTsFileDataTypeConverter =
        new LoadTsFileDataTypeConverter(queryContext, isGeneratedByPipe);

    final Iterator<Integer> iterator = failedTsFileNodeIndexes.listIterator();
    while (iterator.hasNext()) {
      final int failedLoadTsFileIndex = iterator.next();
      final LoadSingleTsFileNode failedNode = tsFileNodeList.get(failedLoadTsFileIndex);
      final String filePath = failedNode.getTsFileResource().getTsFilePath();

      try {
        final TSStatus status =
            failedNode.isTableModel()
                ? loadTsFileDataTypeConverter
                    .convertForTableModel(
                        (isGeneratedByPipe
                                ? LoadTsFile.createForPipe(null, filePath, Collections.emptyMap())
                                : LoadTsFile.createUnchecked(
                                    null, filePath, Collections.emptyMap()))
                            .setDatabase(failedNode.getDatabase())
                            .setDeleteAfterLoad(failedNode.isDeleteAfterLoad())
                            .setConvertOnTypeMismatch(true))
                    .orElse(null)
                : loadTsFileDataTypeConverter
                    .convertForTreeModel(
                        buildRetryTreeLoadStatement(
                            filePath,
                            failedNode.isDeleteAfterLoad(),
                            LoadTsFileScheduler.getPartitionQueryDatabase(
                                failedNode, isGeneratedByPipe)))
                    .orElse(null);

        if (loadTsFileDataTypeConverter.isSuccessful(status)) {
          iterator.remove();
          LOGGER.info(
              DataNodeQueryMessages
                  .LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED,
              failedNode.getTsFileResource().getTsFilePath());
        } else {
          LOGGER.warn(
              DataNodeQueryMessages.LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_STATUS_ARG,
              failedNode.getTsFileResource().getTsFilePath(),
              status);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodeQueryMessages.LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_EXCEPTION_ARG,
            failedNode.getTsFileResource().getTsFilePath(),
            e.getMessage(),
            e);
      }
    }

    // If all failed TsFiles are converted into tablets and inserted,
    // we can consider the load process as successful.
    if (failedTsFileNodeIndexes.isEmpty()) {
      LOGGER.info(DataNodeQueryMessages.LOAD_ALL_FAILED_TSFILES_ARE_CONVERTED_TO_TABLETS);
      stateMachine.transitionToFinished();
    } else {
      final String failedFiles =
          failedTsFileNodeIndexes.stream()
              .map(i -> tsFileNodeList.get(i).getTsFileResource().getTsFilePath())
              .collect(Collectors.joining(", "));
      LOGGER.warn(
          DataNodeQueryMessages
              .LOG_LOAD_FAILED_TO_LOAD_SOME_TSFILES_BY_CONVERTING_THEM_INTO_TABLETS_FAILED_TSFILES_ARG_7D9DB9C3,
          failedFiles);
      stateMachine.transitionToFailed(
          new LoadFileException(
              String.format(
                  DataNodeQueryMessages
                      .LOG_LOAD_FAILED_TO_LOAD_SOME_TSFILES_BY_CONVERTING_THEM_INTO_TABLETS_FAILED_TSFILES_ARG_7D9DB9C3,
                  failedFiles)));
    }
  }

  private LoadTsFileStatement buildRetryTreeLoadStatement(
      final String filePath, final boolean deleteAfterLoad, final String database)
      throws FileNotFoundException {
    final LoadTsFileStatement statement =
        (isGeneratedByPipe
                ? LoadTsFileStatement.createForPipe(filePath)
                : LoadTsFileStatement.createUnchecked(filePath))
            .setDeleteAfterLoad(deleteAfterLoad)
            .setConvertOnTypeMismatch(true);
    if (database != null) {
      statement.setDatabase(database);
      statement.updateDatabaseLevelByTreeDatabase();
    }
    if (isGeneratedByPipe) {
      statement.markIsGeneratedByPipe();
    }
    return statement;
  }
}
