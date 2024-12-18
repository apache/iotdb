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
import org.apache.iotdb.confignode.procedure.state.schema.RenameTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RenameTableProcedure extends AbstractAlterOrDropTableProcedure<RenameTableState> {

  private String newName;

  public RenameTableProcedure() {
    super();
  }

  public RenameTableProcedure(
      final String database, final String tableName, final String queryId, final String newName) {
    super(database, tableName, queryId);
    this.newName = newName;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final RenameTableState renameTableState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final RenameTableState renameTableState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected RenameTableState getState(final int stateId) {
    return RenameTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final RenameTableState state) {
    return state.ordinal();
  }

  @Override
  protected RenameTableState getInitialState() {
    return RenameTableState.COLUMN_CHECK;
  }

  @Override
  protected String getActionMessage() {
    return "rename table";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.RENAME_TABLE_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(newName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.newName = ReadWriteIOUtils.readString(byteBuffer);
  }
}
