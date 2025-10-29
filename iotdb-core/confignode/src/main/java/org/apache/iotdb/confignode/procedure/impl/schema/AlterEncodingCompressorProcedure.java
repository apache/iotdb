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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.AlterEncodingCompressorState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure.invalidateCache;
import static org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure.preparePatternTreeBytesData;

public class AlterEncodingCompressorProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterEncodingCompressorState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlterEncodingCompressorState.class);
  private String queryId;
  private PathPatternTree patternTree;
  private boolean ifExists;
  private byte encoding;
  private byte compressor;
  private boolean mayAlterAudit;

  private transient ByteBuffer patternTreeBytes;
  private transient String requestMessage;

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
    setPatternTree(pathPatternTree);
    this.ifExists = ifExists;
    this.encoding = encoding;
    this.compressor = compressor;
    this.mayAlterAudit = mayAlterAudit;
  }

  public String getQueryId() {
    return queryId;
  }

  @TestOnly
  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public void setPatternTree(final PathPatternTree patternTree) {
    this.patternTree = patternTree;
    requestMessage = patternTree.getAllPathPatterns().toString();
    patternTreeBytes = preparePatternTreeBytesData(patternTree);
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AlterEncodingCompressorState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case ALTER_SCHEMA_REGION:
          LOGGER.info(
              "Alter encoding {} & compressor {} in schema region for timeSeries {}",
              TSEncoding.deserialize(encoding),
              CompressionType.deserialize(compressor),
              requestMessage);
          break;
        case CLEAR_CACHE:
          LOGGER.info("Invalidate cache of timeSeries {}", requestMessage);
          invalidateCache(env, patternTreeBytes, requestMessage, this::setFailure);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "AlterEncodingCompressor-[{}] costs {}ms",
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env,
      final AlterEncodingCompressorState alterEncodingCompressorState)
      throws IOException, InterruptedException, ProcedureException {
    // Not supported now
  }

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
    setPatternTree(PathPatternTree.deserialize(byteBuffer));
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
