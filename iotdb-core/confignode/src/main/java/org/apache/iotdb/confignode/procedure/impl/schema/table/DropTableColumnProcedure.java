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

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DropTableColumnProcedure
    extends AbstractAlterOrDropTableProcedure<DropTableColumnState> {

  private String columnName;

  public DropTableColumnProcedure() {
    super();
  }

  public DropTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName) {
    super(database, tableName, queryId);
    this.columnName = columnName;
  }

  @Override
  protected String getActionMessage() {
    return "drop table column";
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final DropTableColumnState dropTableColumnState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final DropTableColumnState dropTableColumnState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DropTableColumnState getState(final int stateId) {
    return null;
  }

  @Override
  protected int getStateId(final DropTableColumnState dropTableColumnState) {
    return 0;
  }

  @Override
  protected DropTableColumnState getInitialState() {
    return null;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_TABLE_COLUMN_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(columnName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.columnName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o) && Objects.equals(columnName, ((DropTableColumnProcedure) o).columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columnName);
  }
}
