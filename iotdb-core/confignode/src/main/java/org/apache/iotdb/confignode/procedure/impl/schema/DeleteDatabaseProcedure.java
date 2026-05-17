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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.procedure.MetadataProcedureConflictCheckable;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDatabaseState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCheckInvalidTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckInvalidTimeSeriesResp;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDatabaseState.DELETE_ALIAS_DATA;

public class DeleteDatabaseProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteDatabaseState>
    implements MetadataProcedureConflictCheckable {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatabaseProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TDatabaseSchema deleteDatabaseSchema;
  private transient List<String> physicalPathsForAliasSeries = new ArrayList<>();

  /**
   * Result class for invalid time series check. Contains the list of invalid time series paths and
   * the list of physical paths for alias time series.
   */
  public static class InvalidTimeSeriesCheckResult {
    private final int invalidSeriesCount;
    private final List<String> invalidPaths;
    private final List<String> physicalPathsForAliasSeries;

    public InvalidTimeSeriesCheckResult(
        final int invalidSeriesCount,
        final List<String> invalidPaths,
        final List<String> physicalPathsForAliasSeries) {
      this.invalidSeriesCount = invalidSeriesCount;
      this.invalidPaths = invalidPaths;
      this.physicalPathsForAliasSeries = physicalPathsForAliasSeries;
    }

    public List<String> getInvalidPaths() {
      return invalidPaths;
    }

    public List<String> getPhysicalPathsForAliasSeries() {
      return physicalPathsForAliasSeries;
    }

    public int getInvalidTimeSeriesCount() {
      return invalidPaths.size();
    }

    public int getPhysicalPathsForAliasSeriesCount() {
      return physicalPathsForAliasSeries.size();
    }
  }

  public DeleteDatabaseProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DeleteDatabaseProcedure(
      final TDatabaseSchema deleteDatabaseSchema, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.deleteDatabaseSchema = deleteDatabaseSchema;
  }

  public TDatabaseSchema getDeleteDatabaseSchema() {
    return deleteDatabaseSchema;
  }

  public void setDeleteDatabaseSchema(final TDatabaseSchema deleteDatabaseSchema) {
    this.deleteDatabaseSchema = deleteDatabaseSchema;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DeleteDatabaseState state)
      throws InterruptedException {
    if (deleteDatabaseSchema == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case PRE_DELETE_DATABASE:
          LOG.info(
              "[DeleteDatabaseProcedure] Pre delete database: {}", deleteDatabaseSchema.getName());
          env.preDeleteDatabase(
              PreDeleteDatabasePlan.PreDeleteType.EXECUTE, deleteDatabaseSchema.getName());
          setNextState(DeleteDatabaseState.CHECK_INVALID_TIME_SERIES);
          break;
        case CHECK_INVALID_TIME_SERIES:
          handleCheckInvalidTimeSeries(env);
          if (isFailed()) {
            return Flow.NO_MORE_STATE;
          }
          setNextState(DeleteDatabaseState.INVALIDATE_CACHE);
          break;
        case INVALIDATE_CACHE:
          LOG.info(
              "[DeleteDatabaseProcedure] Invalidate cache of database: {}",
              deleteDatabaseSchema.getName());
          if (env.invalidateCache(deleteDatabaseSchema.getName())) {
            setNextState(DeleteDatabaseState.DELETE_DATABASE_SCHEMA);
          } else {
            setFailure(
                new ProcedureException(
                    ProcedureMessages.DELETEDATABASEPROCEDURE_INVALIDATE_CACHE_FAILED));
          }
          break;
        case DELETE_DATABASE_SCHEMA:
          LOG.info(
              "[DeleteDatabaseProcedure] Delete DatabaseSchema: {}",
              deleteDatabaseSchema.getName());

          // Submit RegionDeleteTasks
          final OfferRegionMaintainTasksPlan dataRegionDeleteTaskOfferPlan =
              new OfferRegionMaintainTasksPlan();
          final List<TRegionReplicaSet> regionReplicaSets =
              env.getAllReplicaSets(deleteDatabaseSchema.getName());
          final List<TRegionReplicaSet> schemaRegionReplicaSets = new ArrayList<>();
          regionReplicaSets.forEach(
              regionReplicaSet -> {
                // Clear heartbeat cache along the way
                env.getConfigManager()
                    .getLoadManager()
                    .removeRegionGroupRelatedCache(regionReplicaSet.getRegionId());

                if (regionReplicaSet
                    .getRegionId()
                    .getType()
                    .equals(TConsensusGroupType.SchemaRegion)) {
                  schemaRegionReplicaSets.add(regionReplicaSet);
                } else {
                  regionReplicaSet
                      .getDataNodeLocations()
                      .forEach(
                          targetDataNode ->
                              dataRegionDeleteTaskOfferPlan.appendRegionMaintainTask(
                                  new RegionDeleteTask(
                                      targetDataNode, regionReplicaSet.getRegionId())));
                }
              });

          if (!dataRegionDeleteTaskOfferPlan.getRegionMaintainTaskList().isEmpty()) {
            // submit async data region delete task
            env.getConfigManager().getConsensusManager().write(dataRegionDeleteTaskOfferPlan);
          }

          // try sync delete schemaengine region
          final DataNodeAsyncRequestContext<TConsensusGroupId, TSStatus> asyncClientHandler =
              new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.DELETE_REGION);
          final Map<Integer, RegionDeleteTask> schemaRegionDeleteTaskMap = new HashMap<>();
          int requestIndex = 0;
          for (final TRegionReplicaSet schemaRegionReplicaSet : schemaRegionReplicaSets) {
            for (final TDataNodeLocation dataNodeLocation :
                schemaRegionReplicaSet.getDataNodeLocations()) {
              asyncClientHandler.putRequest(requestIndex, schemaRegionReplicaSet.getRegionId());
              asyncClientHandler.putNodeLocation(requestIndex, dataNodeLocation);
              schemaRegionDeleteTaskMap.put(
                  requestIndex,
                  new RegionDeleteTask(dataNodeLocation, schemaRegionReplicaSet.getRegionId()));
              requestIndex++;
            }
          }
          if (!schemaRegionDeleteTaskMap.isEmpty()) {
            CnToDnInternalServiceAsyncRequestManager.getInstance()
                .sendAsyncRequestWithRetry(asyncClientHandler);
            for (final Map.Entry<Integer, TSStatus> entry :
                asyncClientHandler.getResponseMap().entrySet()) {
              if (entry.getValue().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                LOG.info(
                    "[DeleteDatabaseProcedure] Successfully delete SchemaRegion[{}] on {}",
                    asyncClientHandler.getRequest(entry.getKey()),
                    schemaRegionDeleteTaskMap.get(entry.getKey()).getTargetDataNode());
                schemaRegionDeleteTaskMap.remove(entry.getKey());
              } else {
                LOG.warn(
                    "[DeleteDatabaseProcedure] Failed to delete SchemaRegion[{}] on {}. Submit to async deletion.",
                    asyncClientHandler.getRequest(entry.getKey()),
                    schemaRegionDeleteTaskMap.get(entry.getKey()).getTargetDataNode());
              }
            }

            if (!schemaRegionDeleteTaskMap.isEmpty()) {
              // submit async schemaengine region delete task for failed sync execution
              final OfferRegionMaintainTasksPlan schemaRegionDeleteTaskOfferPlan =
                  new OfferRegionMaintainTasksPlan();
              schemaRegionDeleteTaskMap
                  .values()
                  .forEach(schemaRegionDeleteTaskOfferPlan::appendRegionMaintainTask);
              env.getConfigManager().getConsensusManager().write(schemaRegionDeleteTaskOfferPlan);
            }
          }

          env.getConfigManager()
              .getLoadManager()
              .clearDataPartitionPolicyTable(deleteDatabaseSchema.getName());
          LOG.info(
              "[DeleteDatabaseProcedure] The data partition policy table of database: {} is cleared.",
              deleteDatabaseSchema.getName());

          // Delete Database metrics
          PartitionMetrics.unbindDatabaseRelatedMetricsWhenUpdate(
              MetricService.getInstance(), deleteDatabaseSchema.getName());

          // Delete DatabasePartitionTable
          final TSStatus deleteConfigResult =
              env.deleteDatabaseConfig(deleteDatabaseSchema.getName(), isGeneratedByPipe);

          if (deleteConfigResult.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOG.info(
                "[DeleteDatabaseProcedure] Database: {} is deleted successfully",
                deleteDatabaseSchema.getName());
            setNextState(DELETE_ALIAS_DATA);
            break;
          } else if (getCycles() > RETRY_THRESHOLD) {
            setFailure(
                new ProcedureException(
                    ProcedureMessages.DELETEDATABASEPROCEDURE_DELETE_DATABASESCHEMA_FAILED));
            return Flow.NO_MORE_STATE;
          }
          break;
        case DELETE_ALIAS_DATA:
          if (physicalPathsForAliasSeries == null || physicalPathsForAliasSeries.isEmpty()) {
            return Flow.NO_MORE_STATE;
          }
          LOG.info(
              "Delete data of physical paths for alias series in database: {}",
              deleteDatabaseSchema.getName());
          deleteDataOfPhysicalPathsForAliasSeries(env);
          break;
        case DELETE_ALIAS_SCHEMA:
          LOG.info(
              "Delete schema of physical paths for alias series in database: {}",
              deleteDatabaseSchema.getName());
          deleteSchemaForPhysicalPathsOfAliasSeries(env);
          if (isFailed()) {
            return Flow.NO_MORE_STATE;
          }
          setNextState(DeleteDatabaseState.INVALIDATE_ALIAS_CACHE);
          break;
        case INVALIDATE_ALIAS_CACHE:
          LOG.info(
              "Invalidate cache of physical paths for alias series in database: {}",
              deleteDatabaseSchema.getName());
          invalidateAliasCache(env);
          if (isFailed()) {
            return Flow.NO_MORE_STATE;
          }
          return Flow.NO_MORE_STATE;
      }
    } catch (final ConsensusException | TException | IOException e) {
      if (isRollbackSupported(state)) {
        setFailure(
            new ProcedureException(
                ProcedureMessages.DELETEDATABASEPROCEDURE_DELETE_DATABASE
                    + deleteDatabaseSchema.getName()
                    + " failed "
                    + state));
      } else {
        LOG.error(
            "[DeleteDatabaseProcedure] Retriable error trying to delete database {}, state {}",
            deleteDatabaseSchema.getName(),
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  ProcedureMessages.DELETEDATABASEPROCEDURE_STATE_STUCK_AT + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final DeleteDatabaseState state)
      throws IOException, InterruptedException {
    switch (state) {
      case PRE_DELETE_DATABASE:
      case CHECK_INVALID_TIME_SERIES:
      case DELETE_ALIAS_DATA:
      case DELETE_ALIAS_SCHEMA:
      case INVALIDATE_ALIAS_CACHE:
      case INVALIDATE_CACHE:
        LOG.info(
            "[DeleteDatabaseProcedure] Rollback to preDeleted: {}", deleteDatabaseSchema.getName());
        env.preDeleteDatabase(
            PreDeleteDatabasePlan.PreDeleteType.ROLLBACK, deleteDatabaseSchema.getName());
        break;
      default:
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(final DeleteDatabaseState state) {
    switch (state) {
      case PRE_DELETE_DATABASE:
      case CHECK_INVALID_TIME_SERIES:
      case DELETE_ALIAS_DATA:
      case DELETE_ALIAS_SCHEMA:
      case INVALIDATE_ALIAS_CACHE:
      case INVALIDATE_CACHE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DeleteDatabaseState getState(final int stateId) {
    return DeleteDatabaseState.values()[stateId];
  }

  @Override
  protected int getStateId(final DeleteDatabaseState deleteDatabaseState) {
    return deleteDatabaseState.ordinal();
  }

  @Override
  protected DeleteDatabaseState getInitialState() {
    return DeleteDatabaseState.PRE_DELETE_DATABASE;
  }

  public String getDatabase() {
    return deleteDatabaseSchema.getName();
  }

  @Override
  public void applyPathPatterns(PathPatternTree patternTree) {
    String databaseName = getDatabase();
    if (databaseName == null
        || databaseName.isEmpty()
        || PathUtils.isTableModelDatabase(databaseName)) {
      return;
    }
    try {
      PartialPath databasePath =
          new PartialPath(databaseName + PATH_SEPARATOR + MULTI_LEVEL_PATH_WILDCARD);
      patternTree.appendPathPattern(databasePath);
    } catch (IllegalPathException e) {
      LOG.warn("Invalid database path: {}", databaseName, e);
    }
  }

  @Override
  public boolean shouldCheckConflict() {
    return !isFinished();
  }

  /**
   * Handle checking invalid time series. This method focuses on execution and validation, leaving
   * state transitions to the switch block.
   *
   * @param env ConfigNodeProcedureEnv
   */
  private void handleCheckInvalidTimeSeries(ConfigNodeProcedureEnv env) {
    final String databaseName = deleteDatabaseSchema.getName();
    LOG.info(
        "[DeleteDatabaseProcedure] checking invalid time series for database: {}", databaseName);

    final InvalidTimeSeriesCheckResult checkResult = checkInvalidTimeSeries(env, databaseName);

    if (checkResult == null) {
      setFailure(
          new ProcedureException(
              String.format("Failed to check invalid time series for database %s", databaseName)));
      return;
    }

    if (checkResult.getInvalidTimeSeriesCount() > 0) {
      setFailure(
          new ProcedureException(new MetadataException(buildInvalidPathErrorMessage(checkResult))));
      return;
    }

    if (checkResult.getPhysicalPathsForAliasSeriesCount() > 0) {
      LOG.info(
          "[DeleteDatabaseProcedure] found {} alias series in database: {}. preparing physical path cleanup.",
          checkResult.getPhysicalPathsForAliasSeriesCount(),
          databaseName);
      this.physicalPathsForAliasSeries = checkResult.getPhysicalPathsForAliasSeries();
    }
  }

  /**
   * Build error message for invalid paths.
   *
   * @param result InvalidTimeSeriesCheckResult
   * @return error message string
   */
  private String buildInvalidPathErrorMessage(InvalidTimeSeriesCheckResult result) {
    final int maxDisplay = 5;
    final List<String> invalidPaths = result.getInvalidPaths();

    String pathInfo =
        invalidPaths.stream().limit(maxDisplay).collect(java.util.stream.Collectors.joining(", "));

    if (invalidPaths.size() > maxDisplay) {
      pathInfo += String.format(" ... and %d more", invalidPaths.size() - maxDisplay);
    }

    return String.format(
        "cannot delete database %s: contains %d invalid time series. sample paths: [%s]",
        deleteDatabaseSchema.getName(), result.getInvalidTimeSeriesCount(), pathInfo);
  }

  /**
   * Check if the database has invalid time series by querying all SchemaRegions in the database.
   *
   * @param env ConfigNodeProcedureEnv
   * @param databaseName database name
   * @return InvalidTimeSeriesCheckResult containing counts and paths
   */
  private InvalidTimeSeriesCheckResult checkInvalidTimeSeries(
      final ConfigNodeProcedureEnv env, final String databaseName) {
    if (PathUtils.isTableModelDatabase(databaseName)) {
      return new InvalidTimeSeriesCheckResult(0, Collections.emptyList(), Collections.emptyList());
    }

    try {
      final List<TRegionReplicaSet> schemaRegions = filterSchemaRegions(env, databaseName);
      if (schemaRegions.isEmpty()) {
        return new InvalidTimeSeriesCheckResult(
            0, Collections.emptyList(), Collections.emptyList());
      }

      final RequestContext requestContext = createCheckRequests(schemaRegions, databaseName);
      if (requestContext.isEmpty()) {
        return new InvalidTimeSeriesCheckResult(
            0, Collections.emptyList(), Collections.emptyList());
      }

      sendCheckRequests(requestContext.handler);
      return processCheckResponses(requestContext, databaseName);
    } catch (final Exception e) {
      LOG.error(
          "[DeleteDatabaseProcedure] Error checking invalid time series for database: {}. "
              + "This will trigger rollback.",
          databaseName,
          e);
      return null;
    }
  }

  private List<TRegionReplicaSet> filterSchemaRegions(
      final ConfigNodeProcedureEnv env, final String databaseName) {
    final List<TRegionReplicaSet> schemaRegions = new ArrayList<>();
    for (final TRegionReplicaSet region : env.getAllReplicaSets(databaseName)) {
      if (region.getRegionId().getType().equals(TConsensusGroupType.SchemaRegion)) {
        schemaRegions.add(region);
      }
    }
    return schemaRegions;
  }

  private static class RequestContext {
    final DataNodeAsyncRequestContext<TCheckInvalidTimeSeriesReq, TCheckInvalidTimeSeriesResp>
        handler;
    final Map<Integer, TRegionReplicaSet> regionMap;

    RequestContext(
        final DataNodeAsyncRequestContext<TCheckInvalidTimeSeriesReq, TCheckInvalidTimeSeriesResp>
            handler,
        final Map<Integer, TRegionReplicaSet> regionMap) {
      this.handler = handler;
      this.regionMap = regionMap;
    }

    boolean isEmpty() {
      return regionMap.isEmpty();
    }
  }

  private RequestContext createCheckRequests(
      final List<TRegionReplicaSet> schemaRegions, final String databaseName) {
    final DataNodeAsyncRequestContext<TCheckInvalidTimeSeriesReq, TCheckInvalidTimeSeriesResp>
        handler =
            new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CHECK_INVALID_TIME_SERIES);
    final Map<Integer, TRegionReplicaSet> regionMap = new HashMap<>();
    int requestIndex = 0;

    for (final TRegionReplicaSet region : schemaRegions) {
      final List<TDataNodeLocation> locations = region.getDataNodeLocations();
      if (locations.isEmpty()) {
        continue;
      }

      try {
        final TCheckInvalidTimeSeriesReq req = new TCheckInvalidTimeSeriesReq();
        req.setSchemaRegionId(region.getRegionId());
        req.setDatabaseName(databaseName);
        handler.putRequest(requestIndex, req);
        handler.putNodeLocation(requestIndex, locations.get(0));
        regionMap.put(requestIndex, region);
        requestIndex++;
      } catch (final Exception e) {
        LOG.error(
            "[DeleteDatabaseProcedure] Error creating request for database: {} on SchemaRegion: {}",
            databaseName,
            region.getRegionId(),
            e);
      }
    }

    return new RequestContext(handler, regionMap);
  }

  private void sendCheckRequests(
      final DataNodeAsyncRequestContext<TCheckInvalidTimeSeriesReq, TCheckInvalidTimeSeriesResp>
          handler) {
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(handler);
  }

  private InvalidTimeSeriesCheckResult processCheckResponses(
      final RequestContext requestContext, final String databaseName) {
    final List<String> invalidPaths = new ArrayList<>();
    final List<String> physicalPathsForAliasSeries = new ArrayList<>();
    int invalidSeriesCount = 0;

    for (final Map.Entry<Integer, TCheckInvalidTimeSeriesResp> entry :
        requestContext.handler.getResponseMap().entrySet()) {
      final TCheckInvalidTimeSeriesResp resp = entry.getValue();
      final TRegionReplicaSet region = requestContext.regionMap.get(entry.getKey());

      if (!isResponseSuccess(resp)) {
        LOG.error(
            "[DeleteDatabaseProcedure] Failed to check invalid time series for database: {} "
                + "on SchemaRegion: {}, status: {}. This will trigger rollback.",
            databaseName,
            region.getRegionId(),
            resp != null ? resp.getStatus() : "null");
        return null;
      }

      if (resp.isSetInvalidTimeSeriesCount() && resp.getInvalidTimeSeriesCount() > 0) {
        invalidPaths.addAll(resp.getInvalidTimeSeriesPaths());
        invalidSeriesCount += resp.getInvalidTimeSeriesCount();
      }

      if (resp.isSetPhysicalPathsForAliasSeries()
          && !resp.getPhysicalPathsForAliasSeries().isEmpty()) {
        physicalPathsForAliasSeries.addAll(resp.getPhysicalPathsForAliasSeries());
      }
    }

    return buildResult(invalidSeriesCount, invalidPaths, physicalPathsForAliasSeries);
  }

  private boolean isResponseSuccess(final TCheckInvalidTimeSeriesResp resp) {
    return resp != null
        && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private InvalidTimeSeriesCheckResult buildResult(
      final int invalidSeriesCount,
      final List<String> invalidPaths,
      final List<String> physicalPathsForAliasSeries) {
    return new InvalidTimeSeriesCheckResult(
        invalidSeriesCount, invalidPaths, physicalPathsForAliasSeries);
  }

  private PathPatternTree buildPatternTreeForPhysicalPathsOfAliasSeries() {
    final PathPatternTree patternTree = new PathPatternTree();
    for (final String path : physicalPathsForAliasSeries) {
      try {
        patternTree.appendFullPath(new PartialPath(path));
      } catch (final IllegalPathException e) {
        LOG.warn("[DeleteDatabaseProcedure] Invalid physical path for alias series: {}", path, e);
      }
    }
    patternTree.constructTree();
    return patternTree;
  }

  private void deleteDataOfPhysicalPathsForAliasSeries(final ConfigNodeProcedureEnv env) {
    final PathPatternTree physicalPathsPatternTree =
        buildPatternTreeForPhysicalPathsOfAliasSeries();
    if (physicalPathsPatternTree.isEmpty()) {
      setNextState(DeleteDatabaseState.INVALIDATE_ALIAS_CACHE);
      return;
    }

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup(physicalPathsPatternTree, false);

    if (relatedDataRegionGroup.isEmpty()) {
      setNextState(DeleteDatabaseState.DELETE_ALIAS_SCHEMA);
      return;
    }

    final SchemaUtils.SchemaRegionTaskExecutor<TDeleteDataForDeleteSchemaReq> deleteDataTask =
        new SchemaUtils.SchemaRegionTaskExecutor<>(
            "delete data of physical paths for alias series",
            env,
            relatedDataRegionGroup,
            false,
            CnToDnAsyncRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteDataForDeleteSchemaReq(
                        new ArrayList<>(consensusGroupIdList), physicalPathsPatternTree.serialize())
                    .setIsGeneratedByPipe(isGeneratedByPipe)),
            ((failureMessage, setFailure) -> {
              final ProcedureException exception =
                  new ProcedureException(
                      new MetadataException(
                          String.format(
                              "Delete physical paths for alias series %s", failureMessage)));
              setFailure.accept(exception);
            }),
            this::setFailure);
    deleteDataTask.execute();

    if (isFailed()) {
      return;
    }
    setNextState(DeleteDatabaseState.DELETE_ALIAS_SCHEMA);
  }

  private void deleteSchemaForPhysicalPathsOfAliasSeries(final ConfigNodeProcedureEnv env) {
    if (physicalPathsForAliasSeries == null || physicalPathsForAliasSeries.isEmpty()) {
      return;
    }

    final PathPatternTree physicalPathsPatternTree =
        buildPatternTreeForPhysicalPathsOfAliasSeries();
    if (physicalPathsPatternTree.isEmpty()) {
      return;
    }

    final SchemaUtils.SchemaRegionTaskExecutor<TDeleteTimeSeriesReq> deleteTimeSeriesTask =
        new SchemaUtils.SchemaRegionTaskExecutor<>(
            "delete schema for physical paths of alias series",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(physicalPathsPatternTree, false),
            CnToDnAsyncRequestType.DELETE_TIMESERIES,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteTimeSeriesReq(consensusGroupIdList, physicalPathsPatternTree.serialize())
                    .setIsGeneratedByPipe(isGeneratedByPipe)),
            ((failureMessage, setFailure) -> {
              final ProcedureException exception =
                  new ProcedureException(
                      new MetadataException(
                          String.format(
                              "Delete physical paths for alias series %s", failureMessage)));
              setFailure.accept(exception);
            }),
            this::setFailure);
    deleteTimeSeriesTask.execute();
  }

  private void invalidateAliasCache(final ConfigNodeProcedureEnv env) {
    if (physicalPathsForAliasSeries == null || physicalPathsForAliasSeries.isEmpty()) {
      return;
    }

    final PathPatternTree physicalPathsPatternTree =
        buildPatternTreeForPhysicalPathsOfAliasSeries();
    if (physicalPathsPatternTree.isEmpty()) {
      return;
    }

    final String requestMessage =
        String.format(
            "physical paths for alias series in database %s", deleteDatabaseSchema.getName());

    SchemaUtils.invalidateCache(
        env, physicalPathsPatternTree.serialize(), requestMessage, this::setFailure, true);
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE.getTypeCode()
            : ProcedureType.DELETE_DATABASE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTDatabaseSchema(deleteDatabaseSchema, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      deleteDatabaseSchema = ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(byteBuffer);
    } catch (final ThriftSerDeException e) {
      LOG.error(ProcedureMessages.ERROR_IN_DESERIALIZE_DELETEDATABASEPROCEDURE, e);
    }
  }

  @Override
  public boolean equals(final Object that) {
    if (that instanceof DeleteDatabaseProcedure) {
      final DeleteDatabaseProcedure thatProc = (DeleteDatabaseProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getCurrentState().equals(this.getCurrentState())
          && thatProc.getCycles() == this.getCycles()
          && thatProc.isGeneratedByPipe == this.isGeneratedByPipe
          && thatProc.deleteDatabaseSchema.equals(this.getDeleteDatabaseSchema());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, deleteDatabaseSchema);
  }
}
