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
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeTSStatusTaskExecutor;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

  protected AbstractAlterOrDropTableProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  protected AbstractAlterOrDropTableProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
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
    preRelease(env, null);
  }

  protected void preRelease(final ConfigNodeProcedureEnv env, final @Nullable String oldName) {
    // Proceed once every unreachable DataNode is provably self-fenced instead of hard-failing the
    // DDL: a fenced DataNode fails closed on its now-stale table cache and resyncs on lease
    // recovery, so it cannot serve dirty schema. Only fail if an unacked DataNode is not provably
    // fenced (it may still be serving clients).
    final TUpdateTableReq req = SchemaUtils.buildPreUpdateTableReq(database, table, oldName);
    final boolean proceeded =
        new ClusterCachePropagator(SchemaUtils.filterFencedDataNode(env.getConfigManager()))
            .propagate(targets -> SchemaUtils.broadcastTableUpdate(req, targets));

    if (!proceeded) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_PRE_RELEASE_FOR_TABLE_TO_DATANODE_FAILURE_RESULTS,
          getActionMessage(),
          database,
          table.getTableName(),
          ProcedureMessages.FAILED_TO_PROVE_DN_IS_FENCED);
      setFailure(
          new ProcedureException(
              new MetadataException(
                  ProcedureMessages.PRE_RELEASE
                      + getActionMessage()
                      + ProcedureMessages.EXCEPTION_FAILED_C6FF154E)));
    }
  }

  protected void commitRelease(final ConfigNodeProcedureEnv env) {
    commitRelease(env, null);
  }

  protected void commitRelease(final ConfigNodeProcedureEnv env, final @Nullable String oldName) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.commitReleaseTable(
            database, table.getTableName(), env.getConfigManager(), oldName);
    if (!failedResults.isEmpty()) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_FOR_TABLE_TO_DATANODE_FAILURE_RESULTS,
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
    rollbackPreRelease(env, null);
  }

  protected void rollbackPreRelease(
      final ConfigNodeProcedureEnv env, final @Nullable String tableName) {
    // A down DataNode must not block rollback either: proceed past provably-fenced DataNodes (which
    // resync on recovery) and only fail on an unacked DataNode that is not provably fenced.
    final TUpdateTableReq req =
        SchemaUtils.rollbackUpdateTableReq(database, table.getTableName(), tableName);
    final boolean proceeded =
        new ClusterCachePropagator(SchemaUtils.filterFencedDataNode(env.getConfigManager()))
            .propagate(targets -> SchemaUtils.broadcastTableUpdate(req, targets));

    if (!proceeded) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_ROLLBACK_PRE_RELEASE_FOR_TABLE_INFO_TO_DATANODE,
          getActionMessage(),
          database,
          table.getTableName(),
          ProcedureMessages.FAILED_TO_PROVE_AN_UNREACHABLE_DN_IS_FENCED);
      setFailure(
          new ProcedureException(
              new MetadataException(
                  ProcedureMessages.ROLLBACK_PRE_RELEASE
                      + getActionMessage()
                      + ProcedureMessages.EXCEPTION_FAILED_C6FF154E)));
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

  protected class TableRegionTaskExecutor<Q> extends DataNodeTSStatusTaskExecutor<Q> {

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
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      ProcedureMessages.FOR_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL_REPLICASET,
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
        && Objects.equals(queryId, that.queryId)
        && isGeneratedByPipe == that.isGeneratedByPipe;
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, queryId, isGeneratedByPipe);
  }
}
