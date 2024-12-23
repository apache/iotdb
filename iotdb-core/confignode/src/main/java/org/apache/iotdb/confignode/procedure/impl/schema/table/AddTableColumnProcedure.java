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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.schema.AddTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class AddTableColumnProcedure
    extends AbstractAlterOrDropTableProcedure<AddTableColumnState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AddTableColumnProcedure.class);
  private List<TsTableColumnSchema> addedColumnList;

  public AddTableColumnProcedure() {
    super();
  }

  public AddTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final List<TsTableColumnSchema> addedColumnList) {
    super(database, tableName, queryId);
    this.addedColumnList = addedColumnList;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final AddTableColumnState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case COLUMN_CHECK:
          LOGGER.info("Column check for table {}.{} when adding column", database, tableName);
          columnCheck(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Pre release info of table {}.{} when adding column", database, tableName);
          preRelease(env);
          break;
        case ADD_COLUMN:
          LOGGER.info("Add column to table {}.{}", database, tableName);
          addColumn(env);
          break;
        case COMMIT_RELEASE:
          LOGGER.info("Commit release info of table {}.{} when adding column", database, tableName);
          commitRelease(env);
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

  private void columnCheck(final ConfigNodeProcedureEnv env) {
    try {
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .tableColumnCheckForColumnExtension(database, tableName, addedColumnList);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(
            new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
        return;
      }
      table = result.getRight();
      setNextState(AddTableColumnState.PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  @Override
  protected void preRelease(final ConfigNodeProcedureEnv env) {
    super.preRelease(env);
    setNextState(AddTableColumnState.ADD_COLUMN);
  }

  private void addColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .addTableColumn(database, tableName, addedColumnList);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    } else {
      setNextState(AddTableColumnState.COMMIT_RELEASE);
    }
  }

  @Override
  protected String getActionMessage() {
    return "add table column";
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final AddTableColumnState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case ADD_COLUMN:
          LOGGER.info(
              "Start rollback Add column to table {}.{} when adding column",
              database,
              table.getTableName());
          rollbackAddColumn(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              "Start rollback pre release info of table {}.{}", database, table.getTableName());
          rollbackPreRelease(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback DropTable-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackAddColumn(final ConfigNodeProcedureEnv env) {
    if (table == null) {
      return;
    }
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .rollbackAddTableColumn(database, tableName, addedColumnList);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected AddTableColumnState getState(final int stateId) {
    return AddTableColumnState.values()[stateId];
  }

  @Override
  protected int getStateId(final AddTableColumnState state) {
    return state.ordinal();
  }

  @Override
  protected AddTableColumnState getInitialState() {
    return AddTableColumnState.COLUMN_CHECK;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ADD_TABLE_COLUMN_PROCEDURE.getTypeCode());
    super.serialize(stream);

    TsTableColumnSchemaUtil.serialize(addedColumnList, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.addedColumnList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(this.addedColumnList, ((AddTableColumnProcedure) o).addedColumnList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), addedColumnList);
  }
}
