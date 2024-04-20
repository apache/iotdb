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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCType;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackCreateTablePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.confignode.procedure.state.schema.CreateTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceResp;
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

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

public class CreateTableProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, CreateTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableProcedure.class);

  private String database;

  private TsTable table;

  public CreateTableProcedure() {
    super();
  }

  public CreateTableProcedure(String database, TsTable table) {
    this.database = database;
    this.table = table;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateTableState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_CREATE:
          LOGGER.info("Pre create table {}.{}", database, table.getTableName());
          preCreateTable(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Pre release table {}.{}", database, table.getTableName());
          preReleaseTable(env);
          break;
        case VALIDATE_TIMESERIES_EXISTENCE:
          LOGGER.info(
              "Validate timeseries existence for table {}.{}", database, table.getTableName());
          validateTimeSeriesExistence(env);
          break;
        case COMMIT_CREATE:
          LOGGER.info("Commit create table {}.{}", database, table.getTableName());
          commitCreateTable(env);
          break;
        case COMMIT_RELEASE:
          LOGGER.info("Commit release table {}.{}", database, table.getTableName());
          commitReleaseTable(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized CreateTableState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "CreateTable-{}.{}-{} costs {}ms",
          database,
          table.getTableName(),
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void preCreateTable(ConfigNodeProcedureEnv env) {
    PreCreateTablePlan plan = new PreCreateTablePlan(database, table);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(CreateTableState.PRE_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void preReleaseTable(ConfigNodeProcedureEnv env) {
    TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.PRE_CREATE.getOperationType());
    req.setTableInfo(TsTableInternalRPCUtil.serializeSingleTsTable(database, table));

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TUpdateTableReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to sync table {}.{} pre-create info to DataNode {}",
            database,
            table.getTableName(),
            dataNodeLocationMap.get(entry.getKey()));
        setFailure(new ProcedureException(new MetadataException("Pre create table failed")));
        return;
      }
    }
    setNextState(CreateTableState.VALIDATE_TIMESERIES_EXISTENCE);
  }

  private void validateTimeSeriesExistence(ConfigNodeProcedureEnv env) {
    PathPatternTree patternTree = new PathPatternTree();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    PartialPath path = null;
    try {
      path =
          new PartialPath(ROOT + PATH_SEPARATOR + database + PATH_SEPARATOR + table.getTableName());
      patternTree.appendPathPattern(path);
      patternTree.appendPathPattern(path.concatNode(MULTI_LEVEL_PATH_WILDCARD));
      patternTree.serialize(dataOutputStream);
    } catch (IllegalPathException | IOException ignored) {
    }
    ByteBuffer patternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);

    List<TCheckTimeSeriesExistenceResp> respList = new ArrayList<>();
    DataNodeRegionTaskExecutor<TCheckTimeSeriesExistenceReq, TCheckTimeSeriesExistenceResp>
        regionTask =
            new DataNodeRegionTaskExecutor<
                TCheckTimeSeriesExistenceReq, TCheckTimeSeriesExistenceResp>(
                env,
                relatedSchemaRegionGroup,
                false,
                DataNodeRequestType.CHECK_TIMESERIES_EXISTENCE,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TCheckTimeSeriesExistenceReq(patternTreeBytes, consensusGroupIdList))) {

              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  TDataNodeLocation dataNodeLocation,
                  List<TConsensusGroupId> consensusGroupIdList,
                  TCheckTimeSeriesExistenceResp response) {
                respList.add(response);
                List<TConsensusGroupId> failedRegionList = new ArrayList<>();
                if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  return failedRegionList;
                }

                if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
                  List<TSStatus> subStatus = response.getStatus().getSubStatus();
                  for (int i = 0; i < subStatus.size(); i++) {
                    if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                        && subStatus.get(i).getCode()
                            != TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
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
                                "Create table %s.%s failed when [check timeseries existence on DataNode] because all replicaset of schemaRegion %s failed. %s",
                                database,
                                table.getTableName(),
                                consensusGroupId.id,
                                dataNodeLocationSet))));
                interruptTask();
              }
            };
    regionTask.execute();
    if (isFailed()) {
      return;
    }

    for (TCheckTimeSeriesExistenceResp resp : respList) {
      if (resp.isExists()) {
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        "Timeseries already exists under root.%s.%s",
                        database, table.getTableName()))));
      }
    }
    setNextState(CreateTableState.COMMIT_CREATE);
  }

  private void commitCreateTable(ConfigNodeProcedureEnv env) {
    CommitCreateTablePlan plan = new CommitCreateTablePlan(database, table.getTableName());
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(CreateTableState.COMMIT_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void commitReleaseTable(ConfigNodeProcedureEnv env) {
    TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.COMMIT_CREATE.getOperationType());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      ReadWriteIOUtils.write(table.getTableName(), outputStream);
    } catch (IOException ignored) {
      //
    }
    req.setTableInfo(outputStream.toByteArray());

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TUpdateTableReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to sync table {}.{} commit-create info to DataNode {}",
            database,
            table.getTableName(),
            dataNodeLocationMap.get(entry.getKey()));
        setFailure(new ProcedureException(new MetadataException("Commit create table failed")));
        return;
      }
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateTableState state)
      throws IOException, InterruptedException, ProcedureException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_CREATE:
          LOGGER.info("Start rollback pre create table {}.{}", database, table.getTableName());
          rollbackCreate(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Start rollback pre release table {}.{}", database, table.getTableName());
          rollbackRelease(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback CreateTable-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackCreate(ConfigNodeProcedureEnv env) {
    RollbackCreateTablePlan plan = new RollbackCreateTablePlan(database, table.getTableName());
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to rollback table creation {}.{}", database, table.getTableName());
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void rollbackRelease(ConfigNodeProcedureEnv env) {
    TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.ROLLBACK_CREATE.getOperationType());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      ReadWriteIOUtils.write(table.getTableName(), outputStream);
    } catch (IOException ignored) {
      //
    }
    req.setTableInfo(outputStream.toByteArray());

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TUpdateTableReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to sync table {}.{} rollback-create info to DataNode {}",
            database,
            table.getTableName(),
            dataNodeLocationMap.get(entry.getKey()));
        setFailure(new ProcedureException(new MetadataException("Rollback create table failed")));
        return;
      }
    }
  }

  @Override
  protected CreateTableState getState(int stateId) {
    return CreateTableState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateTableState createTableState) {
    return createTableState.ordinal();
  }

  @Override
  protected CreateTableState getInitialState() {
    return CreateTableState.PRE_CREATE;
  }

  public String getDatabase() {
    return database;
  }

  public TsTable getTable() {
    return table;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_TABLE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    table.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    database = ReadWriteIOUtils.readString(byteBuffer);
    table = TsTable.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CreateTableProcedure)) return false;
    CreateTableProcedure that = (CreateTableProcedure) o;
    return Objects.equals(database, that.database) && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table);
  }
}
