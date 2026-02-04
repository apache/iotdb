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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewColumnPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.ColumnRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.SchemaEvolution;
import org.apache.iotdb.mpp.rpc.thrift.TDataRegionEvolveSchemaReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RenameTableColumnProcedure
    extends AbstractAlterOrDropTableProcedure<RenameTableColumnState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RenameTableColumnProcedure.class);

  private List<String> oldNames;
  private List<String> newNames;

  public RenameTableColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public RenameTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final List<String> oldNames,
      final List<String> newNames,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.oldNames = oldNames;
    this.newNames = newNames;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final RenameTableColumnState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case COLUMN_CHECK:
          LOGGER.info("Column check for table {}.{} when renaming column", database, tableName);
          columnCheck(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Pre release info of table {}.{} when renaming column", database, tableName);
          preRelease(env);
          break;
        case RENAME_COLUMN:
          LOGGER.info("Rename column to table {}.{} on config node", database, tableName);
          renameColumn(env);
          break;
        case EXECUTE_ON_REGION:
          LOGGER.info("Rename column to table {}.{} on data regions", database, tableName);
          executeOnRegions(env);
          break;
        case COMMIT_RELEASE:
          LOGGER.info(
              "Commit release info of table {}.{} when renaming column", database, tableName);
          commitRelease(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized RenameTableColumnState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "RenameTableColumn-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void columnCheck(final ConfigNodeProcedureEnv env) {
    try {
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .tableColumnCheckForColumnRenaming(
                  database,
                  tableName,
                  oldNames,
                  newNames,
                  this instanceof RenameViewColumnProcedure);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(new ProcedureException(new IoTDBException(status)));
        return;
      }
      table = result.getRight();
      setNextState(RenameTableColumnState.PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  @Override
  protected void preRelease(final ConfigNodeProcedureEnv env) {
    super.preRelease(env);
    setNextState(RenameTableColumnState.RENAME_COLUMN);
  }

  private void renameColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                this instanceof RenameViewColumnProcedure
                    ? new RenameViewColumnPlan(database, tableName, oldNames, newNames)
                    : new RenameTableColumnPlan(database, tableName, oldNames, newNames),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(RenameTableColumnState.EXECUTE_ON_REGION);
    }
  }

  private void executeOnRegions(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> relatedRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup4TableModel(database);

    if (!relatedRegionGroup.isEmpty()) {
      List<SchemaEvolution> schemaEvolutions = new ArrayList<>();
      for (int i = 0; i < oldNames.size(); i++) {
        schemaEvolutions.add(new ColumnRename(tableName, oldNames.get(i), newNames.get(i)));
      }
      PublicBAOS publicBAOS = new PublicBAOS();
      try {
        SchemaEvolution.serializeList(schemaEvolutions, publicBAOS);
      } catch (IOException ignored) {
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      new TableRegionTaskExecutor<>(
              "evolve data region schema",
              env,
              relatedRegionGroup,
              CnToDnAsyncRequestType.EVOLVE_DATA_REGION_SCHEMA,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDataRegionEvolveSchemaReq(
                      new ArrayList<>(consensusGroupIdList), byteBuffer)))
          .execute();
    }

    setNextState(RenameTableColumnState.COMMIT_RELEASE);
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final RenameTableColumnState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case RENAME_COLUMN:
          LOGGER.info(
              "Start rollback Renaming column to table {}.{} on configNode",
              database,
              table.getTableName());
          rollbackRenameColumn(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              "Start rollback pre release info of table {}.{}", database, table.getTableName());
          rollbackPreRelease(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback RenameTableColumn-{} costs {}ms.",
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackRenameColumn(final ConfigNodeProcedureEnv env) {
    if (table == null) {
      return;
    }
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                new RenameTableColumnPlan(database, tableName, newNames, oldNames),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  protected RenameTableColumnState getState(final int stateId) {
    return RenameTableColumnState.values()[stateId];
  }

  @Override
  protected int getStateId(final RenameTableColumnState state) {
    return state.ordinal();
  }

  @Override
  protected RenameTableColumnState getInitialState() {
    return RenameTableColumnState.COLUMN_CHECK;
  }

  @Override
  protected String getActionMessage() {
    return "rename table column";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_RENAME_TABLE_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.RENAME_TABLE_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);

    IOUtils.write(oldNames, stream);
    IOUtils.write(newNames, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.oldNames = IOUtils.readStringList(byteBuffer);
    this.newNames = IOUtils.readStringList(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(oldNames, ((RenameTableColumnProcedure) o).oldNames)
        && Objects.equals(newNames, ((RenameTableColumnProcedure) o).newNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), oldNames, newNames);
  }
}
