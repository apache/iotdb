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
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState;
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

public abstract class AbstractSetPropertiesProcedure
    extends AbstractAlterOrDropTableProcedure<SetTablePropertiesState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSetPropertiesProcedure.class);

  protected Map<String, String> originalProperties = new HashMap<>();
  protected Map<String, String> updatedProperties;

  protected AbstractSetPropertiesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  protected AbstractSetPropertiesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final Map<String, String> properties,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.updatedProperties = properties;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final SetTablePropertiesState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case VALIDATE_TABLE:
          validateTable(env);
          LOGGER.info(
              ProcedureMessages.VALIDATE_TABLE_FOR_TABLE_WHEN_SETTING_PROPERTIES,
              database,
              tableName);
          if (!isFailed() && Objects.isNull(table)) {
            LOGGER.info(
                ProcedureMessages.THE_UPDATED_TABLE_HAS_THE_SAME_PROPERTIES_WITH_THE_ORIGINAL);
            return Flow.NO_MORE_STATE;
          }
          break;
        case PRE_RELEASE:
          preRelease(env);
          LOGGER.info(
              ProcedureMessages.PRE_RELEASE_INFO_FOR_TABLE_WHEN_SETTING_PROPERTIES,
              database,
              tableName);
          break;
        case SET_PROPERTIES:
          setProperties(env);
          LOGGER.info(ProcedureMessages.SET_PROPERTIES_TO_TABLE, database, tableName);
          break;
        case COMMIT_RELEASE:
          commitRelease(env);
          LOGGER.info(
              ProcedureMessages.COMMIT_RELEASE_INFO_OF_TABLE_WHEN_SETTING_PROPERTIES,
              database,
              tableName);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(
              new ProcedureException(ProcedureMessages.UNRECOGNIZED_ADDTABLECOLUMNSTATE + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          ProcedureMessages.SETTABLEPROPERTIES_COSTS_MS,
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  public void validateTable(final ConfigNodeProcedureEnv env) {
    try {
      final Pair<TSStatus, TsTable> result = updateProperties(env);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(new ProcedureException(new IoTDBException(status)));
        return;
      }
      table = result.getRight();
      setNextState(PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  protected abstract Pair<TSStatus, TsTable> updateProperties(ConfigNodeProcedureEnv env)
      throws MetadataException;

  @Override
  protected void preRelease(final ConfigNodeProcedureEnv env) {
    super.preRelease(env);
    setNextState(SET_PROPERTIES);
  }

  private void setProperties(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(createSetPropertiesPlan(updatedProperties, false), isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(COMMIT_RELEASE);
    }
  }

  protected abstract ConfigPhysicalPlan createSetPropertiesPlan(
      Map<String, String> properties, boolean isRollback);

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final SetTablePropertiesState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_RELEASE:
          LOGGER.info(
              ProcedureMessages.START_ROLLBACK_PRE_RELEASE_INFO_FOR_TABLE_WHEN_SETTING_PROPERTIES,
              database,
              table.getTableName());
          rollbackPreRelease(env);
          break;
        case SET_PROPERTIES:
          LOGGER.info(
              ProcedureMessages.START_ROLLBACK_SET_PROPERTIES_TO_TABLE,
              database,
              table.getTableName());
          rollbackSetProperties(env);
          break;
      }
    } finally {
      LOGGER.info(
          ProcedureMessages.ROLLBACK_SETTABLEPROPERTIES_COSTS_MS,
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
            .executePlan(createSetPropertiesPlan(originalProperties, true), isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
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

  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
    serializeProperties(stream);
  }

  protected void serializeProperties(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(originalProperties, stream);
    ReadWriteIOUtils.write(updatedProperties, stream);
  }

  protected void deserializeProperties(final ByteBuffer byteBuffer) {
    this.originalProperties = ReadWriteIOUtils.readMap(byteBuffer);
    this.updatedProperties = ReadWriteIOUtils.readMap(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(
            updatedProperties, ((AbstractSetPropertiesProcedure) o).updatedProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), updatedProperties);
  }
}
