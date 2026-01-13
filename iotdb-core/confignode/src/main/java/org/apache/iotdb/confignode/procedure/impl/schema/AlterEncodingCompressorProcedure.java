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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.SerializeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterEncodingCompressorPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.AlterEncodingCompressorState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TAlterEncodingCompressorReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure.invalidateCache;
import static org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure.preparePatternTreeBytesData;

public class AlterEncodingCompressorProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterEncodingCompressorState> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AlterEncodingCompressorProcedure.class);
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
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "Alter encoding {} & compressor {} in schema region for timeSeries {}",
                SerializeUtils.deserializeEncodingNullable(encoding),
                SerializeUtils.deserializeCompressorNullable(compressor),
                requestMessage);
          }
          if (!alterEncodingCompressorInSchemaRegion(env)) {
            return Flow.NO_MORE_STATE;
          }
          break;
        case CLEAR_CACHE:
          LOGGER.info("Invalidate cache of timeSeries {}", requestMessage);
          invalidateCache(env, patternTreeBytes, requestMessage, this::setFailure, false);
          collectPayload4Pipe(env);
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

  private boolean alterEncodingCompressorInSchemaRegion(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, mayAlterAudit);

    if (relatedSchemaRegionGroup.isEmpty()) {
      if (!ifExists) {
        setFailure(
            new ProcedureException(
                new PathNotExistException(
                    patternTree.getAllPathPatterns().stream()
                        .map(PartialPath::getFullPath)
                        .collect(Collectors.toList()),
                    false)));
      }
      return false;
    }

    final DataNodeTSStatusTaskExecutor<TAlterEncodingCompressorReq> alterEncodingCompressorTask =
        new DataNodeTSStatusTaskExecutor<TAlterEncodingCompressorReq>(
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, mayAlterAudit),
            false,
            CnToDnAsyncRequestType.ALTER_ENCODING_COMPRESSOR,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TAlterEncodingCompressorReq(consensusGroupIdList, patternTreeBytes, ifExists)
                    .setCompressor(compressor)
                    .setEncoding(encoding))) {

          @Override
          protected List<TConsensusGroupId> processResponseOfOneDataNode(
              final TDataNodeLocation dataNodeLocation,
              final List<TConsensusGroupId> consensusGroupIdList,
              final TSStatus response) {
            final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
            if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              failureMap.remove(dataNodeLocation);
              return failedRegionList;
            }

            if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
              final List<TSStatus> subStatus = response.getSubStatus();
              for (int i = 0; i < subStatus.size(); i++) {
                if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    && !(subStatus.get(i).getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()
                        && ifExists)) {
                  failedRegionList.add(consensusGroupIdList.get(i));
                }
              }
            } else if (!(response.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()
                && ifExists)) {
              failedRegionList.addAll(consensusGroupIdList);
            }
            if (!failedRegionList.isEmpty()) {
              failureMap.put(dataNodeLocation, RpcUtils.extractFailureStatues(response));
            } else {
              failureMap.remove(dataNodeLocation);
            }
            return failedRegionList;
          }

          @Override
          protected void onAllReplicasetFailure(
              final TConsensusGroupId consensusGroupId,
              final Set<TDataNodeLocation> dataNodeLocationSet) {
            setFailure(
                new ProcedureException(
                    new MetadataException(
                        String.format(
                            "Alter encoding compressor %s in schema regions failed. Failures: %s",
                            requestMessage, printFailureMap()))));
            interruptTask();
          }
        };
    alterEncodingCompressorTask.execute();
    setNextState(AlterEncodingCompressorState.CLEAR_CACHE);
    return true;
  }

  private void collectPayload4Pipe(final ConfigNodeProcedureEnv env) {
    TSStatus result;
    try {
      result =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(
                          new PipeAlterEncodingCompressorPlan(
                              patternTreeBytes, encoding, compressor, mayAlterAudit))
                      : new PipeAlterEncodingCompressorPlan(
                          patternTreeBytes, encoding, compressor, mayAlterAudit));
    } catch (final ConsensusException e) {
      LOGGER.warn(ClusterManager.CONSENSUS_WRITE_ERROR, e);
      result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
    }
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(result.getMessage());
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
    if (this == o) {
      return true;
    }
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
