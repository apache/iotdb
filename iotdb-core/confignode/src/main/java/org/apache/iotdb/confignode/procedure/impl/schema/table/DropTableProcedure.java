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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataOrDevicesForDropTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateTableCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

public class DropTableProcedure extends AbstractAlterOrDropTableProcedure<DropTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableProcedure.class);

  // Transient
  private PathPatternTree patternTree;

  public DropTableProcedure() {
    super();
  }

  public DropTableProcedure(final String database, final String tableName, final String queryId) {
    super(database, tableName, queryId);
  }

  // Not used
  @Override
  protected String getActionMessage() {
    return null;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DropTableState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_TABLE:
          LOGGER.info("Check and invalidate table {}.{} when dropping table", database, tableName);
          checkAndPreDeleteTable(env);
          break;
        case INVALIDATE_CACHE:
          LOGGER.info(
              "Invalidating cache for table {}.{} when dropping table", database, tableName);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Deleting data for table {}.{}", database, tableName);
          deleteData(env);
          break;
        case DELETE_DEVICES:
          LOGGER.info("Deleting devices for table {}.{} when dropping table", database, tableName);
          deleteSchema(env);
          break;
        case DROP_TABLE:
          LOGGER.info("Dropping table {}.{} on configNode", database, tableName);
          dropTable(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized DropTableState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "DropTable-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAndPreDeleteTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new PreDeleteTablePlan(database, tableName), env, LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(DropTableState.INVALIDATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TInvalidateTableCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_TABLE_CACHE,
            new TInvalidateTableCacheReq(database, tableName),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // All dataNodes must clear the related schemaEngine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schemaEngine cache of table {}.{}", database, tableName);
        setFailure(
            new ProcedureException(new MetadataException("Invalidate schemaEngine cache failed")));
        return;
      }
    }

    setNextState(DropTableState.DELETE_DATA);
  }

  private void deleteData(final ConfigNodeProcedureEnv env) {
    patternTree = new PathPatternTree();
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    final PartialPath path;
    try {
      path = new PartialPath(new String[] {ROOT, database.substring(5), tableName});
      patternTree.appendPathPattern(path);
      patternTree.appendPathPattern(path.concatAsMeasurementPath(MULTI_LEVEL_PATH_WILDCARD));
      patternTree.serialize(dataOutputStream);
    } catch (final IOException e) {
      LOGGER.warn("failed to serialize request for table {}.{}", database, table.getTableName(), e);
    }

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup(patternTree, true);

    if (!relatedDataRegionGroup.isEmpty()) {
      new TableRegionTaskExecutor<>(
              "delete data for drop table",
              env,
              relatedDataRegionGroup,
              CnToDnAsyncRequestType.DELETE_DATA_FOR_DROP_TABLE,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDeleteDataOrDevicesForDropTableReq(
                      new ArrayList<>(consensusGroupIdList), tableName)))
          .execute();
    }

    setNextState(DropTableState.DELETE_DEVICES);
  }

  private void deleteSchema(final ConfigNodeProcedureEnv env) {
    if (Objects.isNull(patternTree)) {
      patternTree = new PathPatternTree();
      final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      final PartialPath path;
      try {
        path = new PartialPath(new String[] {ROOT, database.substring(5), tableName});
        patternTree.appendPathPattern(path);
        patternTree.appendPathPattern(path.concatAsMeasurementPath(MULTI_LEVEL_PATH_WILDCARD));
        patternTree.serialize(dataOutputStream);
      } catch (final IOException e) {
        LOGGER.warn(
            "failed to serialize request for table {}.{}", database, table.getTableName(), e);
      }
    }

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, true);

    if (!relatedSchemaRegionGroup.isEmpty()) {
      new TableRegionTaskExecutor<>(
              "delete devices for drop table",
              env,
              relatedSchemaRegionGroup,
              CnToDnAsyncRequestType.DELETE_DEVICES_FOR_DROP_TABLE,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDeleteDataOrDevicesForDropTableReq(
                      new ArrayList<>(consensusGroupIdList), tableName)))
          .execute();
    }

    setNextState(DropTableState.DROP_TABLE);
  }

  private void dropTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new CommitDeleteTablePlan(database, tableName), env, LOGGER);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected boolean isRollbackSupported(final DropTableState state) {
    return false;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv, final DropTableState dropTableState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }

  @Override
  protected DropTableState getState(final int stateId) {
    return DropTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropTableState dropTableState) {
    return dropTableState.ordinal();
  }

  @Override
  protected DropTableState getInitialState() {
    return DropTableState.CHECK_AND_INVALIDATE_TABLE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_TABLE_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }
}
