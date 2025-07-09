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
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackCreateTablePlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.CreateTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.rpc.TSStatusCode.TABLE_ALREADY_EXISTS;

public class CreateTableProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, CreateTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableProcedure.class);

  protected String database;

  protected TsTable table;

  public CreateTableProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public CreateTableProcedure(
      final String database, final TsTable table, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.database = database;
    this.table = table;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final CreateTableState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_TABLE_EXISTENCE:
          LOGGER.info("Check the existence of table {}.{}", database, table.getTableName());
          checkTableExistence(env);
          break;
        case PRE_CREATE:
          LOGGER.info("Pre create table {}.{}", database, table.getTableName());
          preCreateTable(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Pre release table {}.{}", database, table.getTableName());
          preReleaseTable(env);
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

  protected void checkTableExistence(final ConfigNodeProcedureEnv env) {
    try {
      if (env.getConfigManager()
          .getClusterSchemaManager()
          .getTableIfExists(database, table.getTableName())
          .isPresent()) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format("Table '%s.%s' already exists.", database, table.getTableName()),
                    TABLE_ALREADY_EXISTS.getStatusCode())));
      } else {
        final TDatabaseSchema schema =
            env.getConfigManager().getClusterSchemaManager().getDatabaseSchemaByName(database);
        if (!table.getPropValue(TsTable.TTL_PROPERTY).isPresent()
            && schema.isSetTTL()
            && schema.getTTL() != Long.MAX_VALUE) {
          table.addProp(TsTable.TTL_PROPERTY, String.valueOf(schema.getTTL()));
        }
        setNextState(CreateTableState.PRE_CREATE);
      }
    } catch (final MetadataException | DatabaseNotExistsException e) {
      setFailure(new ProcedureException(e));
    }
  }

  protected void preCreateTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(new PreCreateTablePlan(database, table), env, LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(CreateTableState.PRE_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void preReleaseTable(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.preReleaseTable(database, table, env.getConfigManager(), null);

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to sync table {}.{} pre-create info to DataNode, failure results: {}",
          database,
          table.getTableName(),
          failedResults);
      setFailure(new ProcedureException(new MetadataException("Pre create table failed")));
      return;
    }

    setNextState(CreateTableState.COMMIT_CREATE);
  }

  private void commitCreateTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            isGeneratedByPipe
                ? new PipeEnrichedPlan(new CommitCreateTablePlan(database, table.getTableName()))
                : new CommitCreateTablePlan(database, table.getTableName()),
            env,
            LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(CreateTableState.COMMIT_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void commitReleaseTable(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.commitReleaseTable(
            database, table.getTableName(), env.getConfigManager(), null);

    if (!failedResults.isEmpty()) {
      LOGGER.warn(
          "Failed to sync table {}.{} commit-create info to DataNode {}, failure results: ",
          database,
          table.getTableName(),
          failedResults);
    }
  }

  @Override
  protected boolean isRollbackSupported(final CreateTableState state) {
    return true;
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final CreateTableState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_CREATE:
          LOGGER.info("Start rollback pre create table {}.{}", database, table.getTableName());
          rollbackCreate(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Start rollback pre release table {}.{}", database, table.getTableName());
          rollbackPreRelease(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback CreateTable-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  protected void rollbackCreate(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new RollbackCreateTablePlan(database, table.getTableName()), env, LOGGER);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to rollback table creation {}.{}", database, table.getTableName());
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void rollbackPreRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.rollbackPreRelease(
            database, table.getTableName(), env.getConfigManager(), null);

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to sync table {}.{} rollback-create info to DataNode {}, failure results: ",
          database,
          table.getTableName(),
          failedResults);
      setFailure(new ProcedureException(new MetadataException("Rollback create table failed")));
    }
  }

  @Override
  protected CreateTableState getState(final int stateId) {
    return CreateTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final CreateTableState state) {
    return state.ordinal();
  }

  @Override
  protected CreateTableState getInitialState() {
    return CreateTableState.CHECK_TABLE_EXISTENCE;
  }

  public String getDatabase() {
    return database;
  }

  public TsTable getTable() {
    return table;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_CREATE_TABLE_PROCEDURE.getTypeCode()
            : ProcedureType.CREATE_TABLE_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    table.serialize(stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    database = ReadWriteIOUtils.readString(byteBuffer);
    table = TsTable.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateTableProcedure that = (CreateTableProcedure) o;
    return Objects.equals(database, that.database)
        && Objects.equals(table, that.table)
        && isGeneratedByPipe == that.isGeneratedByPipe;
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, isGeneratedByPipe);
  }
}
