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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AlterTimeSeriesDataTypeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TAlterTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class AlterTimeSeriesDataTypeProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterTimeSeriesDataTypeState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AlterTimeSeriesDataTypeProcedure.class);

  private String queryId;

  private MeasurementPath measurementPath;
  private transient ByteBuffer measurementPathBytes;
  private byte operationType;
  private TSDataType dataType;

  public AlterTimeSeriesDataTypeProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterTimeSeriesDataTypeProcedure(
      final String queryId,
      final MeasurementPath measurementPath,
      final byte operationType,
      final TSDataType dataType,
      final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    setMeasurementPath(measurementPath);
    this.operationType = operationType;
    this.dataType = dataType;
  }

  @Override
  protected StateMachineProcedure.Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AlterTimeSeriesDataTypeState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_SERIES:
          LOGGER.info(
              "Check and invalidate series {} when altering time series data type",
              measurementPath.getFullPath());
          checkAndPreAlterTimeSeries();
          break;
        case ALTER_TIME_SERIES_DATA_TYPE:
          LOGGER.info("altering time series {} data type", measurementPath.getFullPath());
          if (!alterTimeSeriesDataType(env)) {
            LOGGER.error("alter time series {} data type failed", measurementPath.getFullPath());
            return Flow.NO_MORE_STATE;
          }
          break;
        case CLEAR_CACHE:
          LOGGER.info(
              "clearing cache after alter time series {} data type", measurementPath.getFullPath());
          PathPatternTree patternTree = new PathPatternTree();
          patternTree.appendPathPattern(measurementPath);
          patternTree.constructTree();
          invalidateCache(
              env,
              preparePatternTreeBytesData(patternTree),
              measurementPath.getFullPath(),
              this::setFailure,
              true);
          collectPayload4Pipe(env);
          break;
        default:
          setFailure(
              new ProcedureException(
                  "Unrecognized AlterTimeSeriesDataTypeProcedure state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "AlterTimeSeriesDataType-{}-[{}] costs {}ms",
          measurementPath.getFullPath(),
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAndPreAlterTimeSeries() {
    if (dataType != null) {
      setNextState(AlterTimeSeriesDataTypeState.ALTER_TIME_SERIES_DATA_TYPE);
    } else {
      setFailure(
          new ProcedureException(
              new MetadataException("Invalid data type cannot be used as a new type")));
    }
  }

  private boolean alterTimeSeriesDataType(final ConfigNodeProcedureEnv env) {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(measurementPath);
    patternTree.constructTree();

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, false);

    if (relatedSchemaRegionGroup.isEmpty()) {
      setFailure(
          new ProcedureException(new PathNotExistException(measurementPath.getFullPath(), false)));
      return false;
    }

    final ByteArrayOutputStream stream = new ByteArrayOutputStream(1);
    try {
      ReadWriteIOUtils.write(dataType, stream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }

    final DataNodeTSStatusTaskExecutor<TAlterTimeSeriesReq> alterTimerSeriesTask =
        new DataNodeTSStatusTaskExecutor<TAlterTimeSeriesReq>(
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, false),
            false,
            CnToDnAsyncRequestType.ALTER_TIMESERIES_DATATYPE,
            ((dataNodeLocation, consensusGroupIdList) -> {
              ByteBuffer measurementPathBuffer = null;
              try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                measurementPath.serialize(baos);
                measurementPathBuffer = ByteBuffer.wrap(baos.toByteArray());
              } catch (IOException ignored) {
                // ByteArrayOutputStream won't throw IOException
              }

              return new TAlterTimeSeriesReq(
                  consensusGroupIdList,
                  queryId,
                  measurementPathBuffer,
                  operationType,
                  ByteBuffer.wrap(stream.toByteArray()));
            })) {

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
                    && !(subStatus.get(i).getCode()
                        == TSStatusCode.PATH_NOT_EXIST.getStatusCode())) {
                  failedRegionList.add(consensusGroupIdList.get(i));
                }
              }
            } else if (!(response.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode())) {
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
                            "Alter timeseries %s data type from %s to %s in schema regions failed. Failures: %s",
                            measurementPath.getFullPath(),
                            measurementPath.getSeriesType(),
                            dataType,
                            printFailureMap()))));
            interruptTask();
          }
        };
    alterTimerSeriesTask.execute();
    setNextState(AlterTimeSeriesDataTypeState.CLEAR_CACHE);
    return true;
  }

  public static void invalidateCache(
      final ConfigNodeProcedureEnv env,
      final ByteBuffer measurementPathBytes,
      final String requestMessage,
      final Consumer<ProcedureException> setFailure,
      final boolean needLock) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TInvalidateMatchedSchemaCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
            new TInvalidateMatchedSchemaCacheReq(measurementPathBytes).setNeedLock(needLock),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // All dataNodes must clear the related schemaEngine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schemaEngine cache of timeSeries {}", requestMessage);
        setFailure.accept(
            new ProcedureException(new MetadataException("Invalidate schemaEngine cache failed")));
        return;
      }
    }
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
                          new PipeAlterTimeSeriesPlan(measurementPath, operationType, dataType))
                      : new PipeAlterTimeSeriesPlan(measurementPath, operationType, dataType));
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
  protected boolean isRollbackSupported(
      final AlterTimeSeriesDataTypeState alterTimeSeriesDataTypeState) {
    return false;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final AlterTimeSeriesDataTypeState alterTimeSeriesDataTypeState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }

  @Override
  protected AlterTimeSeriesDataTypeState getState(final int stateId) {
    return AlterTimeSeriesDataTypeState.values()[stateId];
  }

  @Override
  protected int getStateId(final AlterTimeSeriesDataTypeState alterTimeSeriesDataTypeState) {
    return alterTimeSeriesDataTypeState.ordinal();
  }

  @Override
  protected AlterTimeSeriesDataTypeState getInitialState() {
    return AlterTimeSeriesDataTypeState.CHECK_AND_INVALIDATE_SERIES;
  }

  public String getQueryId() {
    return queryId;
  }

  public MeasurementPath getmeasurementPath() {
    return measurementPath;
  }

  public void setMeasurementPath(final MeasurementPath measurementPath) {
    this.measurementPath = measurementPath;
    measurementPathBytes = prepareMeasurementPathBytesData(measurementPath);
  }

  public static ByteBuffer prepareMeasurementPathBytesData(final MeasurementPath measurementPath) {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      measurementPath.serialize(dataOutputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public static ByteBuffer preparePatternTreeBytesData(final PathPatternTree patternTree) {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ALTER_TIMESERIES_DATATYPE_PROCEDURE.getTypeCode()
            : ProcedureType.ALTER_TIMESERIES_DATATYPE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    measurementPath.serialize(stream);
    ReadWriteIOUtils.write(operationType, stream);
    ReadWriteIOUtils.write(dataType, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    setMeasurementPath(MeasurementPath.deserialize(byteBuffer));
    if (getCurrentState() == AlterTimeSeriesDataTypeState.CLEAR_CACHE) {
      LOGGER.info("Successfully restored, will set mods to the data regions anyway");
    }
    if (byteBuffer.hasRemaining()) {
      operationType = ReadWriteIOUtils.readByte(byteBuffer);
    }
    if (byteBuffer.hasRemaining()) {
      dataType = ReadWriteIOUtils.readDataType(byteBuffer);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AlterTimeSeriesDataTypeProcedure that = (AlterTimeSeriesDataTypeProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getCurrentState().equals(that.getCurrentState())
        && this.getCycles() == getCycles()
        && this.isGeneratedByPipe == that.isGeneratedByPipe
        && this.measurementPath.equals(that.measurementPath)
        && this.operationType == that.operationType
        && this.dataType.equals(that.dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, measurementPath);
  }
}
