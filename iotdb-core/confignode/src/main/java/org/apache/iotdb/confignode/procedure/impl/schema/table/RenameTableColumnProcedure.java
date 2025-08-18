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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewColumnPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RenameTableColumnProcedure
    extends AbstractAlterOrDropTableProcedure<RenameTableColumnState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RenameTableColumnProcedure.class);

  private String oldName;
  private String newName;

  public RenameTableColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public RenameTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String oldName,
      final String newName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.oldName = oldName;
    this.newName = newName;
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
                  database, tableName, oldName, newName, this instanceof RenameViewColumnProcedure);
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
                    ? new RenameViewColumnPlan(database, tableName, oldName, newName)
                    : new RenameTableColumnPlan(database, tableName, oldName, newName),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(RenameTableColumnState.COMMIT_RELEASE);
    }
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
                new RenameTableColumnPlan(database, tableName, newName, oldName),
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

    ReadWriteIOUtils.write(oldName, stream);
    ReadWriteIOUtils.write(newName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.oldName = ReadWriteIOUtils.readString(byteBuffer);
    this.newName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(oldName, ((RenameTableColumnProcedure) o).oldName)
        && Objects.equals(newName, ((RenameTableColumnProcedure) o).newName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), oldName, newName);
  }
}
