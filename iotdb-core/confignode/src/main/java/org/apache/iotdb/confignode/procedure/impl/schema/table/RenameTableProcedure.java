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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.SchemaEvolution;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.TableRename;
import org.apache.iotdb.mpp.rpc.thrift.TDataRegionEvolveSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaRegionEvolveSchemaReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RenameTableProcedure extends AbstractAlterOrDropTableProcedure<RenameTableState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RenameTableProcedure.class);
  private String newName;

  public RenameTableProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public RenameTableProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String newName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.newName = newName;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final RenameTableState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case COLUMN_CHECK:
          LOGGER.info("Column check for table {}.{} when renaming table", database, tableName);
          tableCheck(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Pre release info of table {}.{} when renaming table", database, tableName);
          preRelease(env);
          break;
        case RENAME_TABLE:
          LOGGER.info("Rename table {}.{} on config node", database, tableName);
          renameTable(env);
          break;
        case EXECUTE_ON_REGIONS:
          LOGGER.info("Rename table {}.{} on regions", database, tableName);
          executeOnRegions(env);
          break;
        case COMMIT_RELEASE:
          LOGGER.info(
              "Commit release info of table {}.{} when renaming table", database, tableName);
          commitRelease(env, tableName);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized RenameTableState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "RenameTable-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void tableCheck(final ConfigNodeProcedureEnv env) {
    try {
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .tableCheckForRenaming(
                  database, tableName, newName, this instanceof RenameViewProcedure);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(new ProcedureException(new IoTDBException(status)));
        return;
      }
      table = result.getRight();
      setNextState(RenameTableState.PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  @Override
  protected void preRelease(final ConfigNodeProcedureEnv env) {
    super.preRelease(env, tableName, null);
    setNextState(RenameTableState.RENAME_TABLE);
  }

  private void renameTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                this instanceof RenameViewProcedure
                    ? new RenameViewPlan(database, tableName, newName)
                    : new RenameTablePlan(database, tableName, newName),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(RenameTableState.EXECUTE_ON_REGIONS);
    }
  }

  private void executeOnRegions(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup4TableModel(database);

    if (!relatedDataRegionGroup.isEmpty()) {
      List<SchemaEvolution> schemaEvolutions =
          Collections.singletonList(new TableRename(tableName, newName));
      PublicBAOS publicBAOS = new PublicBAOS();
      try {
        SchemaEvolution.serializeList(schemaEvolutions, publicBAOS);
      } catch (IOException ignored) {
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      new TableRegionTaskExecutor<>(
              "evolve data region schema",
              env,
              relatedDataRegionGroup,
              CnToDnAsyncRequestType.EVOLVE_DATA_REGION_SCHEMA,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDataRegionEvolveSchemaReq(
                      new ArrayList<>(consensusGroupIdList), byteBuffer)))
          .execute();
    }

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup4TableModel(database);

    if (!relatedSchemaRegionGroup.isEmpty()) {
      List<SchemaEvolution> schemaEvolutions =
          Collections.singletonList(new TableRename(tableName, newName));
      PublicBAOS publicBAOS = new PublicBAOS();
      try {
        SchemaEvolution.serializeList(schemaEvolutions, publicBAOS);
      } catch (IOException ignored) {
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      new TableRegionTaskExecutor<>(
              "evolve schema region schema",
              env,
              relatedSchemaRegionGroup,
              CnToDnAsyncRequestType.EVOLVE_SCHEMA_REGION_SCHEMA,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TSchemaRegionEvolveSchemaReq(
                      new ArrayList<>(consensusGroupIdList), byteBuffer)))
          .execute();
    }

    invalidateAuthCache(env);

    setNextState(RenameTableState.COMMIT_RELEASE);
  }

  private void invalidateAuthCache(final ConfigNodeProcedureEnv env) {
    TInvalidatePermissionCacheReq req = new TInvalidatePermissionCacheReq();
    // use all empty to invalidate all cache
    req.setUsername("");
    req.setRoleName("");
    TSStatus status;
    List<TDataNodeConfiguration> allDataNodes =
        env.getConfigManager().getNodeManager().getRegisteredDataNodes();
    List<Pair<TDataNodeConfiguration, Long>> dataNodesToInvalid = new ArrayList<>();
    for (TDataNodeConfiguration item : allDataNodes) {
      dataNodesToInvalid.add(new Pair<>(item, System.currentTimeMillis()));
    }
    Iterator<Pair<TDataNodeConfiguration, Long>> it = dataNodesToInvalid.iterator();
    long timeoutMS = 10 * 60 * 1000; // 10 minutes
    while (it.hasNext()) {
      Pair<TDataNodeConfiguration, Long> pair = it.next();
      if (pair.getRight() + timeoutMS < System.currentTimeMillis()) {
        LOGGER.error(
            "invalidateAuthCache: timeout on {}, may need clear cache manually",
            pair.getLeft().getLocation());
        it.remove();
        continue;
      }
      status =
          (TSStatus)
              SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithRetry(
                      pair.getLeft().getLocation().getInternalEndPoint(),
                      req,
                      CnToDnSyncRequestType.INVALIDATE_PERMISSION_CACHE);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        it.remove();
      }
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final RenameTableState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case RENAME_TABLE:
          LOGGER.info(
              "Start rollback Renaming table {}.{} on configNode", database, table.getTableName());
          rollbackRenameTable(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              "Start rollback pre release info of table {}.{}", database, table.getTableName());
          rollbackPreRelease(env, tableName);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback RenameTable-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackRenameTable(final ConfigNodeProcedureEnv env) {
    if (table == null) {
      return;
    }
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                this instanceof RenameViewProcedure
                    ? new RenameViewPlan(database, newName, tableName)
                    : new RenameTablePlan(database, newName, tableName),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  public String getNewName() {
    return newName;
  }

  @Override
  protected RenameTableState getState(final int stateId) {
    return RenameTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final RenameTableState state) {
    return state.ordinal();
  }

  @Override
  protected RenameTableState getInitialState() {
    return RenameTableState.COLUMN_CHECK;
  }

  @Override
  protected String getActionMessage() {
    return "rename table";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_RENAME_TABLE_PROCEDURE.getTypeCode()
            : ProcedureType.RENAME_TABLE_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);

    ReadWriteIOUtils.write(newName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.newName = ReadWriteIOUtils.readString(byteBuffer);
  }
}
