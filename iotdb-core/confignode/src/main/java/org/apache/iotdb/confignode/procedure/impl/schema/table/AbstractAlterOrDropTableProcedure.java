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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public abstract class AbstractAlterOrDropTableProcedure<T>
    extends StateMachineProcedure<ConfigNodeProcedureEnv, T> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractAlterOrDropTableProcedure.class);

  protected String database;
  protected String tableName;
  protected String queryId;

  protected TsTable table;

  protected AbstractAlterOrDropTableProcedure() {
    super();
  }

  protected AbstractAlterOrDropTableProcedure(
      final String database, final String tableName, final String queryId) {
    this.database = database;
    this.tableName = tableName;
    this.queryId = queryId;
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

  protected void preRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.preReleaseTable(database, table, env.getConfigManager());

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to pre-release {} for table {}.{} to DataNode, failure results: {}",
          getActionMessage(),
          database,
          table.getTableName(),
          failedResults);
      setFailure(
          new ProcedureException(
              new MetadataException("Pre-release " + getActionMessage() + " failed")));
    }
  }

  protected void commitRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.commitReleaseTable(database, table.getTableName(), env.getConfigManager());
    if (!failedResults.isEmpty()) {
      LOGGER.warn(
          "Failed to {} for table {}.{} to DataNode, failure results: {}",
          getActionMessage(),
          database,
          table.getTableName(),
          failedResults);
    }
  }

  @Override
  protected boolean isRollbackSupported(final T state) {
    return true;
  }

  protected void rollbackPreRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.rollbackPreRelease(database, table.getTableName(), env.getConfigManager());

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to rollback pre-release {} for table {}.{} info to DataNode, failure results: {}",
          getActionMessage(),
          database,
          table.getTableName(),
          failedResults);
      setFailure(
          new ProcedureException(
              new MetadataException("Rollback pre-release " + getActionMessage() + " failed")));
    }
  }

  protected abstract String getActionMessage();

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);

    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(queryId, stream);

    if (Objects.nonNull(table)) {
      ReadWriteIOUtils.write(true, stream);
      table.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.database = ReadWriteIOUtils.readString(byteBuffer);
    this.tableName = ReadWriteIOUtils.readString(byteBuffer);
    this.queryId = ReadWriteIOUtils.readString(byteBuffer);

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      this.table = TsTable.deserialize(byteBuffer);
    }
  }

  protected class TableRegionTaskExecutor<Q> extends DataNodeRegionTaskExecutor<Q, TSStatus> {

    private final String taskName;

    protected TableRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup,
        final CnToDnAsyncRequestType dataNodeRequestType,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(env, targetRegionGroup, false, dataNodeRequestType, dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        final TSStatus response) {
      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return failedRegionList;
      }

      if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatus = response.getSubStatus();
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
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "[%s] for %s.%s failed when [%s] because failed to execute in all replicaset of %s %s. Failure nodes: %s",
                      this.getClass().getSimpleName(),
                      database,
                      tableName,
                      taskName,
                      consensusGroupId.type,
                      consensusGroupId.id,
                      dataNodeLocationSet))));
      interruptTask();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AbstractAlterOrDropTableProcedure<?> that = (AbstractAlterOrDropTableProcedure<?>) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, queryId);
  }
}
