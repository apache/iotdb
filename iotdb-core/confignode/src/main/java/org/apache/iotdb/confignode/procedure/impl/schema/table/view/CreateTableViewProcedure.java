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

package org.apache.iotdb.confignode.procedure.impl.schema.table.view;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.write.table.view.PreCreateTableViewPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.persistence.schema.TreeDeviceViewFieldDetector;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.CreateTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.rpc.TSStatusCode.TABLE_ALREADY_EXISTS;

public class CreateTableViewProcedure extends CreateTableProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableViewProcedure.class);
  private boolean replace;
  private TsTable oldView;
  private TableNodeStatus oldStatus;

  public CreateTableViewProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public CreateTableViewProcedure(
      final String database,
      final TsTable table,
      final boolean replace,
      final boolean isGeneratedByPipe) {
    super(database, table, isGeneratedByPipe);
    this.replace = replace;
  }

  @Override
  protected void checkTableExistence(final ConfigNodeProcedureEnv env) {
    if (!replace) {
      super.checkTableExistence(env);
    } else {
      try {
        final Optional<Pair<TsTable, TableNodeStatus>> oldTableAndStatus =
            env.getConfigManager()
                .getClusterSchemaManager()
                .getTableAndStatusIfExists(database, table.getTableName());
        if (oldTableAndStatus.isPresent()) {
          if (!TreeViewSchema.isTreeViewTable(oldTableAndStatus.get().getLeft())) {
            setFailure(
                new ProcedureException(
                    new IoTDBException(
                        String.format(
                            "Table '%s.%s' already exists.", database, table.getTableName()),
                        TABLE_ALREADY_EXISTS.getStatusCode())));
            return;
          } else {
            oldView = oldTableAndStatus.get().getLeft();
            oldStatus = oldTableAndStatus.get().getRight();
            setNextState(CreateTableState.PRE_CREATE);
          }
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
    final TSStatus status =
        new TreeDeviceViewFieldDetector(env.getConfigManager(), table, null)
            .detectMissingFieldTypes();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  protected void preCreateTable(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new PreCreateTableViewPlan(database, table, TableNodeStatus.PRE_CREATE), env, LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(CreateTableState.PRE_RELEASE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  protected void rollbackCreate(final ConfigNodeProcedureEnv env) {
    if (Objects.isNull(oldView)) {
      super.rollbackCreate(env);
      return;
    }
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new PreCreateTableViewPlan(database, oldView, oldStatus), env, LOGGER);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to rollback table creation {}.{}", database, table.getTableName());
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_CREATE_TABLE_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.CREATE_TABLE_VIEW_PROCEDURE.getTypeCode());
    innerSerialize(stream);
    ReadWriteIOUtils.write(replace, stream);

    ReadWriteIOUtils.write(Objects.nonNull(oldView), stream);
    if (Objects.nonNull(oldView)) {
      oldView.serialize(stream);
    }

    ReadWriteIOUtils.write(Objects.nonNull(oldStatus), stream);
    if (Objects.nonNull(oldStatus)) {
      oldStatus.serialize(stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    replace = ReadWriteIOUtils.readBool(byteBuffer);

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      this.oldView = TsTable.deserialize(byteBuffer);
    }

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      this.oldStatus = TableNodeStatus.deserialize(byteBuffer);
    }
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && replace == ((CreateTableViewProcedure) o).replace
        && Objects.equals(oldView, ((CreateTableViewProcedure) o).oldView)
        && Objects.equals(oldStatus, ((CreateTableViewProcedure) o).oldStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), replace, oldView, oldStatus);
  }
}
