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

import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.AlterEncodingCompressorState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class AlterEncodingCompressorProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterEncodingCompressorState> {
  private String queryId;
  private PathPatternTree patternTree;
  private boolean ifExists;
  private byte encoding;
  private byte compressor;
  private boolean mayAlterAudit;

  public AlterEncodingCompressorProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterEncodingCompressorProcedure(
      final boolean isGeneratedByPipe,
      final String queryId,
      final PathPatternTree pathPatternTree,
      final boolean ifExists,
      final byte encoding,
      final byte compressor,
      final boolean mayAlterAudit) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    this.patternTree = pathPatternTree;
    this.ifExists = ifExists;
    this.encoding = encoding;
    this.compressor = compressor;
    this.mayAlterAudit = mayAlterAudit;
  }

  public String getQueryId() {
    return queryId;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env,
      final AlterEncodingCompressorState alterEncodingCompressorState)
      throws InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env,
      final AlterEncodingCompressorState alterEncodingCompressorState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected AlterEncodingCompressorState getState(final int stateId) {
    return AlterEncodingCompressorState.values()[stateId];
  }

  @Override
  protected int getStateId(final AlterEncodingCompressorState alterEncodingCompressorState) {
    return alterEncodingCompressorState.ordinal();
  }

  @Override
  protected AlterEncodingCompressorState getInitialState() {
    return AlterEncodingCompressorState.ALTER_SCHEMA_REGION;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ALTER_ENCODING_COMPRESSOR_PROCEDURE.getTypeCode()
            : ProcedureType.ALTER_ENCODING_COMPRESSOR_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    patternTree.serialize(stream);
    ReadWriteIOUtils.write(ifExists, stream);
    ReadWriteIOUtils.write(encoding, stream);
    ReadWriteIOUtils.write(compressor, stream);
    ReadWriteIOUtils.write(mayAlterAudit, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    patternTree = PathPatternTree.deserialize(byteBuffer);
    ifExists = ReadWriteIOUtils.readBoolean(byteBuffer);
    encoding = ReadWriteIOUtils.readByte(byteBuffer);
    compressor = ReadWriteIOUtils.readByte(byteBuffer);
    mayAlterAudit = ReadWriteIOUtils.readBoolean(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AlterEncodingCompressorProcedure that = (AlterEncodingCompressorProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getCurrentState().equals(that.getCurrentState())
        && this.getCycles() == getCycles()
        && Objects.equals(this.queryId, that.queryId)
        && this.isGeneratedByPipe == that.isGeneratedByPipe
        && this.patternTree.equals(that.patternTree)
        && this.encoding == that.encoding
        && this.compressor == that.compressor
        && this.mayAlterAudit == that.mayAlterAudit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        queryId,
        isGeneratedByPipe,
        patternTree,
        ifExists,
        encoding,
        compressor,
        mayAlterAudit);
  }
}
