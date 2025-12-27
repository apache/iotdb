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
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.AlterTableColumnDataTypeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class AlterTableColumnDataTypeProcedure
    extends AbstractAlterOrDropTableProcedure<AlterTableColumnDataTypeState> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AlterTableColumnDataTypeProcedure.class);

  private String columnName;
  private TSDataType dataType;

  public AlterTableColumnDataTypeProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterTableColumnDataTypeProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final TSDataType dataType,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.columnName = columnName;
    this.dataType = dataType;
  }

  @Override
  protected String getActionMessage() {
    return "Alter table column data type";
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AlterTableColumnDataTypeState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_COLUMN:
          LOGGER.info(
              "Check and invalidate column {} in {}.{} when altering column data type",
              columnName,
              database,
              tableName);
          checkAndPreAlterColumn(env);
          break;
        case PRE_RELEASE:
          LOGGER.info("Pre-release info of table {}.{} when altering column", database, tableName);
          preRelease(env);
          break;
        case ALTER_TABLE_COLUMN_DATA_TYPE:
          LOGGER.info("Altering column {} in {}.{} on configNode", columnName, database, tableName);
          alterColumnDataType(env);
          break;
        case COMMIT_RELEASE:
          LOGGER.info(
              "Commit release info of table {}.{} when altering column", database, tableName);
          commitRelease(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(
              new ProcedureException("Unrecognized AlterTableColumnDataTypeProcedure " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "AlterTableColumnDataType-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  @Override
  protected void preRelease(ConfigNodeProcedureEnv env) {
    super.preRelease(env);
    setNextState(AlterTableColumnDataTypeState.ALTER_TABLE_COLUMN_DATA_TYPE);
  }

  private void checkAndPreAlterColumn(final ConfigNodeProcedureEnv env) {
    try {
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .tableColumnCheckForColumnAltering(database, tableName, columnName, dataType);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(
            new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
        return;
      }
      table = result.getRight();
      setNextState(AlterTableColumnDataTypeState.PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  private void alterColumnDataType(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new AlterColumnDataTypePlan(database, tableName, columnName, dataType), env, LOGGER);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
    setNextState(AlterTableColumnDataTypeState.COMMIT_RELEASE);
  }

  @Override
  protected boolean isRollbackSupported(final AlterTableColumnDataTypeState state) {
    return false;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final AlterTableColumnDataTypeState alterTableColumnDataTypeState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }

  @Override
  protected AlterTableColumnDataTypeState getState(final int stateId) {
    return AlterTableColumnDataTypeState.values()[stateId];
  }

  @Override
  protected int getStateId(final AlterTableColumnDataTypeState alterTableColumnDataTypeState) {
    return alterTableColumnDataTypeState.ordinal();
  }

  @Override
  protected AlterTableColumnDataTypeState getInitialState() {
    return AlterTableColumnDataTypeState.CHECK_AND_INVALIDATE_COLUMN;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ALTER_COLUMN_DATATYPE_PROCEDURE.getTypeCode()
            : ProcedureType.ALTER_TABLE_COLUMN_DATATYPE_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(columnName, stream);
    ReadWriteIOUtils.write(dataType, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.columnName = ReadWriteIOUtils.readString(byteBuffer);
    this.dataType = ReadWriteIOUtils.readDataType(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(columnName, ((AlterTableColumnDataTypeProcedure) o).columnName)
        && Objects.equals(dataType, ((AlterTableColumnDataTypeProcedure) o).dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columnName, dataType);
  }
}
