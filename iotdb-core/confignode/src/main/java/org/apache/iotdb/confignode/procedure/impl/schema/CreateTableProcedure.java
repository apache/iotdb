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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.CreateTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreateTableProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, CreateTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableProcedure.class);

  private String database;

  private TsTable table;

  public CreateTableProcedure() {
    super();
  }

  public CreateTableProcedure(String database, TsTable table) {
    this.database = database;
    this.table = table;
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, CreateTableState createTableState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, CreateTableState createTableState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected CreateTableState getState(int stateId) {
    return CreateTableState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateTableState createTableState) {
    return createTableState.ordinal();
  }

  @Override
  protected CreateTableState getInitialState() {
    return CreateTableState.PRE_CREATE;
  }

  public String getDatabase() {
    return database;
  }

  public TsTable getTable() {
    return table;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_TABLE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    table.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    database = ReadWriteIOUtils.readString(byteBuffer);
    table = TsTable.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CreateTableProcedure)) return false;
    CreateTableProcedure that = (CreateTableProcedure) o;
    return Objects.equals(database, that.database) && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table);
  }
}
