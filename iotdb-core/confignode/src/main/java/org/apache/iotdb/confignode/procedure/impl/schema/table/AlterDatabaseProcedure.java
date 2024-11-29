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
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.AlterDatabaseState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
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
import java.util.stream.Collectors;

public class AlterDatabaseProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterDatabaseState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlterDatabaseProcedure.class);

  protected TDatabaseSchema schema;
  protected List<TsTable> tables = new ArrayList<>();

  public AlterDatabaseProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterDatabaseProcedure(final TDatabaseSchema schema, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.schema = schema;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final AlterDatabaseState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_ALTERED_TABLES:
          checkAlteredTables(env);
          LOGGER.info("Checking altered tables when altering database {}", schema.getName());
          break;
        case PRE_RELEASE:
          preRelease(env);
          LOGGER.info(
              "Pre release info for tables {} when altering database {}", tables, schema.getName());
          break;
        case ALTER_DATABASE:
          alterDatabase(env);
          LOGGER.info("Altering database to {}", schema);
          break;
        case COMMIT_RELEASE:
          commitRelease(env);
          LOGGER.info(
              "Commit release info for tables {} when altering database {}",
              tables,
              schema.getName());
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized AddTableColumnState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "AlterDatabase-{}-{} costs {}ms",
          schema.getName(),
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAlteredTables(final ConfigNodeProcedureEnv env) {}

  private void preRelease(final ConfigNodeProcedureEnv env) {
    if (!tables.isEmpty()) {
      final Map<Integer, TSStatus> failedResults =
          SchemaUtils.preReleaseTables(schema.getName(), tables, env.getConfigManager());

      if (!failedResults.isEmpty()) {
        // All dataNodes must clear the related schema cache
        LOGGER.warn(
            "Failed to pre-release tables for alter database {} for altered tables {} to DataNode, failure results: {}",
            schema.getName(),
            tables,
            failedResults);
        setFailure(
            new ProcedureException(
                new MetadataException("Pre-release tables for alter database failed")));
      }
    }

    setNextState(AlterDatabaseState.ALTER_DATABASE);
  }

  private void alterDatabase(final ConfigNodeProcedureEnv env) {
    final DatabaseSchemaPlan plan =
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase, schema);
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            isGeneratedByPipe ? new PipeEnrichedPlan(plan) : plan, env, LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(AlterDatabaseState.COMMIT_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void commitRelease(final ConfigNodeProcedureEnv env) {
    if (tables.isEmpty()) {
      return;
    }
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.commitOrRollbackReleaseTables(
            schema.getName(),
            tables.stream().map(TsTable::getTableName).collect(Collectors.toList()),
            env.getConfigManager(),
            false);
    if (!failedResults.isEmpty()) {
      LOGGER.warn(
          "Failed to commit release tables for alter database {} for altered tables {} to DataNode, failure results: {}",
          schema.getName(),
          tables,
          failedResults);
    }
  }

  public TDatabaseSchema getSchema() {
    return schema;
  }

  @Override
  protected boolean isRollbackSupported(final AlterDatabaseState state) {
    return true;
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final AlterDatabaseState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      // TODO: Config write?
      switch (state) {
        case PRE_RELEASE:
          LOGGER.info(
              "Start rollback pre release info for tables {} when altering database {}",
              tables,
              schema.getName());
          rollbackPreRelease(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback SetTableProperties-{} costs {}ms.",
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  protected void rollbackPreRelease(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TSStatus> failedResults =
        SchemaUtils.commitOrRollbackReleaseTables(
            schema.getName(),
            tables.stream().map(TsTable::getTableName).collect(Collectors.toList()),
            env.getConfigManager(),
            true);

    if (!failedResults.isEmpty()) {
      // All dataNodes must clear the related schema cache
      LOGGER.warn(
          "Failed to rollback pre-release tables for alter database {} for altered tables {} to DataNode, failure results: {}",
          schema.getName(),
          tables,
          failedResults);
      setFailure(
          new ProcedureException(
              new MetadataException("Rollback pre-release tables for alter database failed")));
    }
  }

  @Override
  protected AlterDatabaseState getState(final int stateId) {
    return AlterDatabaseState.values()[stateId];
  }

  @Override
  protected int getStateId(final AlterDatabaseState alterDatabaseState) {
    return alterDatabaseState.ordinal();
  }

  @Override
  protected AlterDatabaseState getInitialState() {
    return AlterDatabaseState.CHECK_ALTERED_TABLES;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ALTER_DATABASE_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ThriftConfigNodeSerDeUtils.serializeTDatabaseSchema(schema, stream);
    ReadWriteIOUtils.write(tables.size(), stream);
    for (final TsTable table : tables) {
      table.serialize(stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      schema = ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(byteBuffer);
    } catch (final ThriftSerDeException e) {
      LOGGER.error("Error in deserialize AlterDatabaseProcedure", e);
    }

    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    this.tables = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      this.tables.add(TsTable.deserialize(byteBuffer));
    }
  }
}
