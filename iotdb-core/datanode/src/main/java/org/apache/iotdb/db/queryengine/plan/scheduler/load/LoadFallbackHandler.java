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
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Fallback execution handler for the LOAD scheduler. When direct chunk staging fails, converts
 * failed TsFiles into In-Memory Tablets, retries insertion through the regular write pipeline, and
 * updates query state accordingly.
 */
public class LoadFallbackHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadFallbackHandler.class);

  private static final LoadTsFileCostMetricsSet COST_METRICS =
      LoadTsFileCostMetricsSet.getInstance();

  private final MPPQueryContext queryContext;
  private final boolean isGeneratedByPipe;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final List<Integer> failedTsFileNodeIndexes;
  private final QueryStateMachine stateMachine;

  public LoadFallbackHandler(
      final MPPQueryContext queryContext,
      final boolean isGeneratedByPipe,
      final List<LoadSingleTsFileNode> tsFileNodeList,
      final List<Integer> failedTsFileNodeIndexes,
      final QueryStateMachine stateMachine) {
    this.queryContext =
        Objects.requireNonNull(
            queryContext, DataNodeQueryMessages.EXCEPTION_QUERYCONTEXT_CANNOT_BE_NULL_C2B25B22);
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.tsFileNodeList =
        Objects.requireNonNull(
            tsFileNodeList, DataNodeQueryMessages.EXCEPTION_TSFILENODELIST_CANNOT_BE_NULL_7562FDB4);
    this.failedTsFileNodeIndexes =
        Objects.requireNonNull(
            failedTsFileNodeIndexes,
            DataNodeQueryMessages.EXCEPTION_FAILEDTSFILENODEINDEXES_CANNOT_BE_NULL_D1C0E7C6);
    this.stateMachine =
        Objects.requireNonNull(
            stateMachine, DataNodeQueryMessages.EXCEPTION_STATEMACHINE_CANNOT_BE_NULL_4AF40790);
  }

  // -------------------------------------------------------------------------
  // Conversion Entrypoint
  // -------------------------------------------------------------------------

  /** Orchestrates fallback conversion of failed TsFiles into Tablets with metric tracking. */
  public void convertFailedTsFilesToTablets() {
    if (failedTsFileNodeIndexes.isEmpty()) {
      stateMachine.transitionToFinished();
      return;
    }

    final String initialFailedFiles = getFailedFilePathsString();
    LOGGER.info(
        DataNodeQueryMessages
            .LOAD_TSFILE_S_FAILED_WILL_TRY_TO_CONVERT_TO_TABLETS_AND_INSERT_FAILED_TSFILES_ARG,
        initialFailedFiles);

    final long startTime = System.nanoTime();
    try {
      convertAndRetry();
    } finally {
      COST_METRICS.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.SCHEDULER_CAST_TABLETS, System.nanoTime() - startTime);
    }
  }

  // -------------------------------------------------------------------------
  // Conversion Loop & State Transition
  // -------------------------------------------------------------------------

  private void convertAndRetry() {
    final LoadTsFileDataTypeConverter converter =
        new LoadTsFileDataTypeConverter(queryContext, isGeneratedByPipe);

    final Iterator<Integer> iterator = failedTsFileNodeIndexes.iterator();
    while (iterator.hasNext()) {
      final int nodeIndex = iterator.next();
      if (nodeIndex < 0 || nodeIndex >= tsFileNodeList.size()) {
        LOGGER.warn(
            DataNodeQueryMessages.LOG_ILLEGAL_FAILED_NODE_INDEX_ARG_OUT_OF_BOUNDS_0_ARG_285B9862,
            nodeIndex,
            tsFileNodeList.size());
        continue;
      }

      final LoadSingleTsFileNode failedNode = tsFileNodeList.get(nodeIndex);
      final String filePath = failedNode.getTsFileResource().getTsFilePath();

      try {
        final TSStatus status =
            failedNode.isTableModel()
                ? executeTableModelConversion(converter, failedNode, filePath)
                : executeTreeModelConversion(converter, failedNode, filePath);

        if (converter.isSuccessful(status)) {
          iterator.remove();
          LOGGER.info(
              DataNodeQueryMessages
                  .LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED,
              filePath);
        } else {
          LOGGER.warn(
              DataNodeQueryMessages.LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_STATUS_ARG,
              filePath,
              status);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodeQueryMessages.LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_EXCEPTION_ARG,
            filePath,
            e.getMessage(),
            e);
      }
    }

    resolveFinalState();
  }

  private TSStatus executeTableModelConversion(
      final LoadTsFileDataTypeConverter converter,
      final LoadSingleTsFileNode node,
      final String filePath) {
    final LoadTsFile statement =
        (isGeneratedByPipe
                ? LoadTsFile.createForPipe(null, filePath, Collections.emptyMap())
                : LoadTsFile.createUnchecked(null, filePath, Collections.emptyMap()))
            .setDatabase(node.getDatabase())
            .setDeleteAfterLoad(node.isDeleteAfterLoad())
            .setConvertOnTypeMismatch(true);

    return converter.convertForTableModel(statement).orElse(null);
  }

  private TSStatus executeTreeModelConversion(
      final LoadTsFileDataTypeConverter converter,
      final LoadSingleTsFileNode node,
      final String filePath)
      throws FileNotFoundException {
    final String database = LoadTsFileScheduler.getPartitionQueryDatabase(node, isGeneratedByPipe);
    final LoadTsFileStatement statement =
        buildRetryTreeLoadStatement(filePath, node.isDeleteAfterLoad(), database);

    return converter.convertForTreeModel(statement).orElse(null);
  }

  private void resolveFinalState() {
    if (failedTsFileNodeIndexes.isEmpty()) {
      LOGGER.info(DataNodeQueryMessages.LOAD_ALL_FAILED_TSFILES_ARE_CONVERTED_TO_TABLETS);
      stateMachine.transitionToFinished();
      return;
    }

    final String remainingFailedFiles = getFailedFilePathsString();
    LOGGER.warn(
        DataNodeQueryMessages
            .LOG_LOAD_FAILED_TO_LOAD_SOME_TSFILES_BY_CONVERTING_THEM_INTO_TABLETS_FAILED_TSFILES_ARG_7D9DB9C3,
        remainingFailedFiles);

    stateMachine.transitionToFailed(
        new LoadFileException(
            String.format(
                DataNodeQueryMessages
                    .LOG_LOAD_FAILED_TO_LOAD_SOME_TSFILES_BY_CONVERTING_THEM_INTO_TABLETS_FAILED_TSFILES_ARG_7D9DB9C3,
                remainingFailedFiles)));
  }

  // -------------------------------------------------------------------------
  // Helper Methods
  // -------------------------------------------------------------------------

  private String getFailedFilePathsString() {
    return failedTsFileNodeIndexes.stream()
        .filter(index -> index >= 0 && index < tsFileNodeList.size())
        .map(index -> tsFileNodeList.get(index).getTsFileResource().getTsFilePath())
        .collect(Collectors.joining(", "));
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
