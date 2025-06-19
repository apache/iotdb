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
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
          LOGGER.info("Rename column to table {}.{} on config node", database, tableName);
          renameTable(env);
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
        setFailure(
            new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
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
    super.preRelease(env, tableName);
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
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    } else {
      setNextState(RenameTableState.COMMIT_RELEASE);
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
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
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
