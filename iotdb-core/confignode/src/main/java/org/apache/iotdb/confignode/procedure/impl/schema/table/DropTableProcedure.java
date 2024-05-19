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
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCType;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDropTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDropTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackDropTablePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public class DropTableProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DropTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableProcedure.class);

  private String database;

  private String tableName;

  private String queryId;

  public DropTableProcedure() {}

  public DropTableProcedure(String database, String tableName, String queryId) {
    this.database = database;
    this.tableName = tableName;
    this.queryId = queryId;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DropTableState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_DROP:
          LOGGER.info("Pre drop table {}.{}", database, tableName);
          preDropTable(env);
          break;
        case INVALIDATE_CACHE:
          LOGGER.info("Invalidate cache of table {}.{}", database, tableName);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of table {}.{}", database, tableName);
          deleteTableData(env);
          break;
        case DROP_TABLE:
          LOGGER.info("Commit drop table {}.{}", database, tableName);
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

  private void preDropTable(ConfigNodeProcedureEnv env) {
    PreDropTablePlan plan = new PreDropTablePlan(database, tableName);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(DropTableState.INVALIDATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();

    TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.INVALIDATE_CACHE.getOperationType());
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, stream);
      ReadWriteIOUtils.write(tableName, stream);
    } catch (IOException ignored) {
      // won't happen
    }
    req.setTableInfo(stream.toByteArray());

    AsyncClientHandler<TUpdateTableReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schema cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate cache of table {}.{}", database, tableName);
        setFailure(new ProcedureException(new MetadataException("Invalidate table cache failed")));
        return;
      }
    }
    setNextState(DropTableState.DELETE_DATA);
  }

  private void deleteTableData(ConfigNodeProcedureEnv env) {
    PartialPath tableTsPath = new PartialPath(new String[] {"root", database, tableName, "**"});
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(tableTsPath);
    patternTree.constructTree();

    deleteDataInDataRegions(env, patternTree);
    deleteSchemaInSchemaRegions(env, patternTree);
    setNextState(DropTableState.DROP_TABLE);
  }

  private void deleteDataInDataRegions(ConfigNodeProcedureEnv env, PathPatternTree patternTree) {
    Map<TConsensusGroupId, TRegionReplicaSet> targetDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup(patternTree);
    if (targetDataRegionGroup.isEmpty()) {
      return;
    }
    DropTableRegionTaskExecutor<TUpdateTableReq> deleteDataTask =
        new DropTableRegionTaskExecutor<>(
            "delete data",
            env,
            targetDataRegionGroup,
            true,
            DataNodeRequestType.UPDATE_TABLE,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TUpdateTableReq(
                    TsTableInternalRPCType.DELETE_DATA_IN_DATA_REGION.getOperationType(),
                    serializeDeleteDataReq(database, tableName, targetDataRegionGroup))));
    deleteDataTask.execute();
  }

  private void deleteSchemaInSchemaRegions(
      ConfigNodeProcedureEnv env, PathPatternTree patternTree) {
    Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);
    if (targetSchemaRegionGroup.isEmpty()) {
      return;
    }
    DropTableRegionTaskExecutor<TUpdateTableReq> deleteDataTask =
        new DropTableRegionTaskExecutor<>(
            "delete schema",
            env,
            targetSchemaRegionGroup,
            false,
            DataNodeRequestType.UPDATE_TABLE,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TUpdateTableReq(
                    TsTableInternalRPCType.DELETE_SCHEMA_IN_SCHEMA_REGION.getOperationType(),
                    serializeDeleteDataReq(database, tableName, targetSchemaRegionGroup))));
    deleteDataTask.execute();
  }

  private ByteBuffer serializeDeleteDataReq(
      String database,
      String tableName,
      Map<TConsensusGroupId, TRegionReplicaSet> targetRegionMap) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, stream);
      ReadWriteIOUtils.write(tableName, stream);
      ReadWriteIOUtils.write(targetRegionMap.size(), stream);
      for (TConsensusGroupId id : targetRegionMap.keySet()) {
        ReadWriteIOUtils.write(id.getId(), stream);
      }
    } catch (IOException ignored) {
      // won't happen
    }
    return ByteBuffer.wrap(stream.toByteArray());
  }

  private void dropTable(ConfigNodeProcedureEnv env) {
    CommitDropTablePlan plan = new CommitDropTablePlan(database, tableName);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DropTableState state)
      throws IOException, InterruptedException, ProcedureException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_DROP:
          rollbackPreDrop(env);
          break;
        case INVALIDATE_CACHE:
          rollbackInvalidateCache(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback DropTable-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackInvalidateCache(ConfigNodeProcedureEnv env) {
    TsTable table = env.getConfigManager().getClusterSchemaManager().getTable(database, tableName);
    TSStatus status = env.getConfigManager().getProcedureManager().createTable(database, table);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || status.getCode() == TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()) {
      return;
    }
    LOGGER.warn(
        "Error occurred during rollback table cache for DropTable {}.{}: {}",
        database,
        tableName,
        status);
    setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
  }

  private void rollbackPreDrop(ConfigNodeProcedureEnv env) {
    RollbackDropTablePlan plan = new RollbackDropTablePlan(database, tableName);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected DropTableState getState(int stateId) {
    return DropTableState.values()[stateId];
  }

  @Override
  protected int getStateId(DropTableState dropTableState) {
    return dropTableState.ordinal();
  }

  @Override
  protected DropTableState getInitialState() {
    return DropTableState.PRE_DROP;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public String getQueryId() {
    return queryId;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_TABLE_PROCEDURE.getTypeCode());
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(queryId, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.database = ReadWriteIOUtils.readString(byteBuffer);
    this.tableName = ReadWriteIOUtils.readString(byteBuffer);
    this.queryId = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DropTableProcedure)) return false;
    DropTableProcedure that = (DropTableProcedure) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, queryId);
  }

  private class DropTableRegionTaskExecutor<Q> extends DataNodeRegionTaskExecutor<Q, TSStatus> {

    private final String taskName;

    DropTableRegionTaskExecutor(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        boolean executeOnAllReplicaset,
        DataNodeRequestType dataNodeRequestType,
        BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(
          env,
          targetSchemaRegionGroup,
          executeOnAllReplicaset,
          dataNodeRequestType,
          dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        TDataNodeLocation dataNodeLocation,
        List<TConsensusGroupId> consensusGroupIdList,
        TSStatus response) {
      List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return failedRegionList;
      }

      if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        List<TSStatus> subStatus = response.getSubStatus();
        for (int i = 0; i < subStatus.size(); i++) {
          if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedRegionList.add(consensusGroupIdList.get(i));
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
    }

    @Override
    protected void onAllReplicasetFailure(
        TConsensusGroupId consensusGroupId, Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Drop table %s.%s failed when [%s] because all replicaset of schemaRegion %s failed. %s",
                      database, tableName, taskName, consensusGroupId.id, dataNodeLocationSet))));
      interruptTask();
    }
  }
}
