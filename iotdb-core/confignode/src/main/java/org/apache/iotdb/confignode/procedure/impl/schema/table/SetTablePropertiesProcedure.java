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
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState.COMMIT_RELEASE;
import static org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState.PRE_RELEASE;
import static org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState.SET_PROPERTIES;
import static org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState.VALIDATE_TABLE;

public class SetTablePropertiesProcedure
    extends AbstractAlterOrDropTableProcedure<SetTablePropertiesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SetTablePropertiesProcedure.class);

  private Map<String, String> originalProperties = new HashMap<>();
  private Map<String, String> updatedProperties;

  public SetTablePropertiesProcedure() {
    super();
  }

  public SetTablePropertiesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final Map<String, String> properties) {
    super(database, tableName, queryId);
    this.updatedProperties = properties;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final SetTablePropertiesState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case VALIDATE_TABLE:
          validateTable(env);
          LOGGER.info(
              "Validate table for table {}.{} when setting properties", database, tableName);
          if (!isFailed() && Objects.isNull(table)) {
            LOGGER.info(
                "The updated table has the same properties with the original one. Skip the procedure.");
            return Flow.NO_MORE_STATE;
          }
          break;
        case PRE_RELEASE:
          preRelease(env);
          LOGGER.info(
              "Pre release info for table {}.{} when setting properties", database, tableName);
          break;
        case SET_PROPERTIES:
          setProperties(env);
          LOGGER.info("Set properties to table {}.{}", database, tableName);
          break;
        case COMMIT_RELEASE:
          commitRelease(env);
          LOGGER.info(
              "Commit release info of table {}.{} when setting properties", database, tableName);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized AddTableColumnState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "SetTableProperties-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void validateTable(final ConfigNodeProcedureEnv env) {
    try {
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .updateTableProperties(database, tableName, originalProperties, updatedProperties);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(
            new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
        return;
      }
      table = result.getRight();
      setNextState(PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  @Override
  protected void preRelease(final ConfigNodeProcedureEnv env) {
    super.preRelease(env);
    setNextState(SET_PROPERTIES);
  }

  private void setProperties(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .setTableProperties(database, tableName, updatedProperties);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    } else {
      setNextState(COMMIT_RELEASE);
    }
  }

  @Override
  protected String getActionMessage() {
    return "set table properties";
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final SetTablePropertiesState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_RELEASE:
          LOGGER.info(
              "Start rollback pre release info for table {}.{} when setting properties",
              database,
              table.getTableName());
          rollbackPreRelease(env);
          break;
        case SET_PROPERTIES:
          LOGGER.info(
              "Start rollback set properties to table {}.{}", database, table.getTableName());
          rollbackSetProperties(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback SetTableProperties-{} costs {}ms.",
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackSetProperties(final ConfigNodeProcedureEnv env) {
    if (table == null) {
      return;
    }
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .setTableProperties(database, tableName, originalProperties);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected SetTablePropertiesState getState(final int stateId) {
    return SetTablePropertiesState.values()[stateId];
  }

  @Override
  protected int getStateId(final SetTablePropertiesState state) {
    return state.ordinal();
  }

  @Override
  protected SetTablePropertiesState getInitialState() {
    return VALIDATE_TABLE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(originalProperties, stream);
    ReadWriteIOUtils.write(updatedProperties, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.originalProperties = ReadWriteIOUtils.readMap(byteBuffer);
    this.updatedProperties = ReadWriteIOUtils.readMap(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(updatedProperties, ((SetTablePropertiesProcedure) o).updatedProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), updatedProperties);
  }
}
