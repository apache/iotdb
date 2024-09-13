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
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.AddTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private List<TsTableColumnSchema> addedColumnList;
  private TsTable table;

  public AddTableColumnProcedure() {}

  public AddTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final List<TsTableColumnSchema> addedColumnList) {
    this.database = database;
    this.tableName = tableName;
    this.queryId = queryId;
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
    final Pair<TSStatus, TsTable> result =
        env.getConfigManager()
            .getClusterSchemaManager()
            .tableColumnCheckForColumnExtension(database, tableName, addedColumnList);
    final TSStatus status = result.getLeft();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
      return;
    }
    table = result.getRight();
    setNextState(AddTableColumnState.PRE_RELEASE);
  }

  private void preRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.preReleaseTable(database, table, env.getConfigManager());

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to pre-release column extension info of table {}.{} to DataNode, failure results: {}",
          database,
          table.getTableName(),
          failedResults);
      setFailure(
          new ProcedureException(
              new MetadataException("Pre-release table column extension info failed")));
      return;
    }

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

  private void commitRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.commitReleaseTable(database, table.getTableName(), env.getConfigManager());
    if (!failedResults.isEmpty()) {
      LOGGER.warn(
          "Failed to commit column extension info of table {}.{} to DataNode, failure results: {}",
          database,
          table.getTableName(),
          failedResults);
      // TODO: Handle commit failure
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final AddTableColumnState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case ADD_COLUMN:
          LOGGER.info(
              "Start rollback pre release info of table {}.{} when adding column",
              database,
              table.getTableName());
          rollbackAddColumn(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Start rollback Add column to table {}.{}", database, table.getTableName());
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

  private void rollbackPreRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.rollbackPreRelease(database, table.getTableName(), env.getConfigManager());

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to rollback pre-release column extension info of table {}.{} info to DataNode, failure results: {}",
          database,
          table.getTableName(),
          failedResults);
      setFailure(
          new ProcedureException(
              new MetadataException("Rollback pre-release table column extension info failed")));
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
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ADD_TABLE_COLUMN_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(queryId, stream);

    TsTableColumnSchemaUtil.serialize(addedColumnList, stream);
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

    this.addedColumnList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(byteBuffer);
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      this.table = TsTable.deserialize(byteBuffer);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AddTableColumnProcedure)) {
      return false;
    }
    final AddTableColumnProcedure that = (AddTableColumnProcedure) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, queryId);
  }
}
