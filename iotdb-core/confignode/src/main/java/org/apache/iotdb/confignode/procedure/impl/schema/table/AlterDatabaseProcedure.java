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

import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AlterDatabaseState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AlterDatabaseState alterDatabaseState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final AlterDatabaseState alterDatabaseState)
      throws IOException, InterruptedException, ProcedureException {}

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
