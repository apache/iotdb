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
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class DeleteDevicesProcedure extends AbstractAlterOrDropTableProcedure<DeleteDevicesState> {
  private byte[] filterBytes;

  public DeleteDevicesProcedure() {
    super();
  }

  public DeleteDevicesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final byte[] filterBytes) {
    super(database, tableName, queryId);
    this.filterBytes = filterBytes;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DeleteDevicesState deleteDevicesState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final DeleteDevicesState deleteDevicesState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DeleteDevicesState getState(final int stateId) {
    return DeleteDevicesState.values()[stateId];
  }

  @Override
  protected int getStateId(final DeleteDevicesState deleteDevicesState) {
    return deleteDevicesState.ordinal();
  }

  @Override
  protected DeleteDevicesState getInitialState() {
    return DeleteDevicesState.CONSTRUCT_BLACK_LIST;
  }

  @Override
  protected String getActionMessage() {
    // Not used
    return null;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DELETE_DEVICES_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(filterBytes.length, stream);
    stream.write(filterBytes);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    byte[] bytes = new byte[ReadWriteIOUtils.readInt(byteBuffer)];
    byteBuffer.get(bytes);
    filterBytes = bytes;
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Arrays.equals(this.filterBytes, ((DeleteDevicesProcedure) o).filterBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), Arrays.hashCode(filterBytes));
  }
}
