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
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DropTableProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DropTableState> {

  protected String database;
  protected String tableName;
  protected String queryId;

  public DropTableProcedure() {}

  public DropTableProcedure(final String database, final String tableName, final String queryId) {
    this.database = database;
    this.tableName = tableName;
    this.queryId = queryId;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv, final DropTableState dropTableState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv, final DropTableState dropTableState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DropTableState getState(final int stateId) {
    return DropTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropTableState dropTableState) {
    return dropTableState.ordinal();
  }

  @Override
  protected DropTableState getInitialState() {
    return DropTableState.CHECK_TABLE_EXISTENCE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);

    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(queryId, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.database = ReadWriteIOUtils.readString(byteBuffer);
    this.tableName = ReadWriteIOUtils.readString(byteBuffer);
    this.queryId = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DropTableProcedure that = (DropTableProcedure) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, queryId);
  }
}
