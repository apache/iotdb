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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCType;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AddTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AddTableColumnProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AddTableColumnState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AddTableColumnProcedure.class);

  private String database;

  private String tableName;

  private String queryId;

  private List<TsTableColumnSchema> inputColumnList;

  private List<TsTableColumnSchema> actualAddedColumnList;

  public AddTableColumnProcedure() {}

  public AddTableColumnProcedure(
      String database,
      String tableName,
      String queryId,
      List<TsTableColumnSchema> inputColumnList) {
    this.database = database;
    this.tableName = tableName;
    this.queryId = queryId;
    this.inputColumnList = inputColumnList;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AddTableColumnState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case ADD_COLUMN:
          LOGGER.info("Add column to table {}.{}", database, tableName);
          addColumn(env);
          break;
        case UPDATE_CACHE:
          LOGGER.info("Update cache of table {}.{} when adding column", database, tableName);
          updateCache(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized AddTableColumnState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "AddTableColumn-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void addColumn(ConfigNodeProcedureEnv env) {
    Pair<TSStatus, List<TsTableColumnSchema>> result =
        env.getConfigManager()
            .getClusterSchemaManager()
            .addTableColumn(database, tableName, inputColumnList);
    TSStatus status = result.getLeft();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
      return;
    }
    actualAddedColumnList = result.getRight();
    setNextState(AddTableColumnState.UPDATE_CACHE);
  }

  private void updateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();

    TUpdateTableReq req =
        new TUpdateTableReq(
            TsTableInternalRPCType.ADD_COLUMN.getOperationType(), getCacheRequestInfo());

    AsyncClientHandler<TUpdateTableReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schema cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to update cache of table {}.{}", database, tableName);
        setFailure(new ProcedureException(new MetadataException("Update table cache failed")));
        return;
      }
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, AddTableColumnState state)
      throws IOException, InterruptedException, ProcedureException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case ADD_COLUMN:
          rollbackAddColumn(env);
          break;
        case UPDATE_CACHE:
          rollbackUpdateCache(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback DropTable-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackAddColumn(ConfigNodeProcedureEnv env) {
    if (actualAddedColumnList == null) {
      return;
    }
    TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .rollbackAddTableColumn(database, tableName, actualAddedColumnList);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void rollbackUpdateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();

    TUpdateTableReq req =
        new TUpdateTableReq(
            TsTableInternalRPCType.ROLLBACK_ADD_COLUMN.getOperationType(), getCacheRequestInfo());

    AsyncClientHandler<TUpdateTableReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schema cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to rollback cache of table {}.{}", database, tableName);
        setFailure(new ProcedureException(new MetadataException("Rollback table cache failed")));
        return;
      }
    }
  }

  private ByteBuffer getCacheRequestInfo() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, stream);
      ReadWriteIOUtils.write(tableName, stream);

      TsTableColumnSchemaUtil.serialize(actualAddedColumnList, stream);
    } catch (IOException ignored) {
      // won't happen
    }
    return ByteBuffer.wrap(stream.toByteArray());
  }

  @Override
  protected AddTableColumnState getState(int stateId) {
    return AddTableColumnState.values()[stateId];
  }

  @Override
  protected int getStateId(AddTableColumnState state) {
    return state.ordinal();
  }

  @Override
  protected AddTableColumnState getInitialState() {
    return AddTableColumnState.ADD_COLUMN;
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

  public List<TsTableColumnSchema> getInputColumnList() {
    return inputColumnList;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ADD_TABLE_COLUMN_PROCEDURE.getTypeCode());

    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(queryId, stream);

    TsTableColumnSchemaUtil.serialize(inputColumnList, stream);
    TsTableColumnSchemaUtil.serialize(actualAddedColumnList, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.database = ReadWriteIOUtils.readString(byteBuffer);
    this.tableName = ReadWriteIOUtils.readString(byteBuffer);
    this.queryId = ReadWriteIOUtils.readString(byteBuffer);

    this.inputColumnList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(byteBuffer);
    this.actualAddedColumnList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AddTableColumnProcedure)) return false;
    AddTableColumnProcedure that = (AddTableColumnProcedure) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, queryId);
  }
}
