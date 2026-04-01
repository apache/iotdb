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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeTSStatusTaskExecutor;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TTableDeviceDeletionWithPatternAndFilterReq;
import org.apache.iotdb.mpp.rpc.thrift.TTableDeviceDeletionWithPatternOrModReq;
import org.apache.iotdb.mpp.rpc.thrift.TTableDeviceInvalidateCacheReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.CHECK_TABLE_EXISTENCE;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.CLEAN_DATANODE_SCHEMA_CACHE;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.CONSTRUCT_BLACK_LIST;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.DELETE_DATA;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.DELETE_DEVICE_SCHEMA;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_NOT_EXISTS;

public class DeleteDevicesProcedure extends AbstractAlterOrDropTableProcedure<DeleteDevicesState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteDevicesProcedure.class);
  private byte[] patternBytes;
  private byte[] filterBytes;
  private byte[] modBytes;

  // Transient, will not be returned if once recovers
  private long deletedDevicesNum;

  public DeleteDevicesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DeleteDevicesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final @Nonnull byte[] patternBytes,
      final @Nonnull byte[] filterBytes,
      final @Nonnull byte[] modBytes,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, isGeneratedByPipe);
    this.patternBytes = patternBytes;
    this.filterBytes = filterBytes;
    this.modBytes = modBytes;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DeleteDevicesState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_TABLE_EXISTENCE:
          LOGGER.info("Check the existence of table {}.{}", database, tableName);
          checkTableExistence(env);
          break;
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schemaEngine black list of devices in {}.{}", database, tableName);
          constructBlackList(env);
          if (deletedDevicesNum > 0) {
            setNextState(CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            return Flow.NO_MORE_STATE;
          }
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of devices in {}.{}", database, tableName);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of devices in {}.{}", database, tableName);
          deleteData(env);
          break;
        case DELETE_DEVICE_SCHEMA:
          LOGGER.info("Delete devices in {}.{} in schemaEngine", database, tableName);
          deleteDeviceSchema(env);
          collectPayload4Pipe(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info("DeleteDevices-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void checkTableExistence(final ConfigNodeProcedureEnv env) {
    try {
      if (!env.getConfigManager()
          .getClusterSchemaManager()
          .getTableIfExists(database, tableName)
          .isPresent()) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format("Table '%s.%s' not exists.", database, tableName),
                    TABLE_NOT_EXISTS.getStatusCode())));
      } else {
        setNextState(CONSTRUCT_BLACK_LIST);
      }
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  private void constructBlackList(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup4TableModel(database);

    if (relatedSchemaRegionGroup.isEmpty()) {
      deletedDevicesNum = 0;
      return;
    }
    final DataNodeTSStatusTaskExecutor<TTableDeviceDeletionWithPatternAndFilterReq>
        deleteDevicesExecutor =
            new DataNodeTSStatusTaskExecutor<TTableDeviceDeletionWithPatternAndFilterReq>(
                env,
                relatedSchemaRegionGroup,
                false,
                CnToDnAsyncRequestType.CONSTRUCT_TABLE_DEVICE_BLACK_LIST,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TTableDeviceDeletionWithPatternAndFilterReq(
                        new ArrayList<>(consensusGroupIdList),
                        tableName,
                        ByteBuffer.wrap(patternBytes),
                        ByteBuffer.wrap(filterBytes)))) {
              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  final TDataNodeLocation dataNodeLocation,
                  final List<TConsensusGroupId> consensusGroupIdList,
                  final TSStatus response) {
                return processResponseOfOneDataNodeWithSuccessResult(
                    dataNodeLocation, consensusGroupIdList, response);
              }

              @Override
              protected void onAllReplicasetFailure(
                  final TConsensusGroupId consensusGroupId,
                  final Set<TDataNodeLocation> dataNodeLocationSet) {
                setFailure(
                    new ProcedureException(
                        new MetadataException(
                            String.format(
                                "[%s] for %s.%s failed when construct black list for table because failed to execute in all replicaset of %s %s. Failures: %s",
                                this.getClass().getSimpleName(),
                                database,
                                tableName,
                                consensusGroupId.type,
                                consensusGroupId.id,
                                printFailureMap()))));
                interruptTask();
              }
            };
    deleteDevicesExecutor.execute();

    setNextState(CONSTRUCT_BLACK_LIST);
    deletedDevicesNum =
        !isFailed()
            ? deleteDevicesExecutor.getSuccessResult().stream()
                .mapToLong(resp -> Long.parseLong(resp.getMessage()))
                .reduce(Long::sum)
                .orElse(0L)
            : 0;
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TTableDeviceInvalidateCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_MATCHED_TABLE_DEVICE_CACHE,
            new TTableDeviceInvalidateCacheReq(database, tableName, ByteBuffer.wrap(patternBytes)),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // All dataNodes must clear the related schemaEngine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to invalidate schemaEngine cache of devices in table {}.{}",
            database,
            tableName);
        setFailure(
            new ProcedureException(new MetadataException("Invalidate schemaEngine cache failed")));
        return;
      }
    }

    setNextState(DELETE_DATA);
  }

  private void deleteData(final ConfigNodeProcedureEnv env) {
    new TableRegionTaskExecutor<>(
            "delete data for table device",
            env,
            env.getConfigManager().getRelatedDataRegionGroup4TableModel(database),
            CnToDnAsyncRequestType.DELETE_DATA_FOR_TABLE_DEVICE,
            (dataNodeLocation, consensusGroupIdList) ->
                new TTableDeviceDeletionWithPatternOrModReq(
                    consensusGroupIdList, tableName, ByteBuffer.wrap(modBytes)))
        .execute();
    setNextState(DELETE_DEVICE_SCHEMA);
  }

  private void deleteDeviceSchema(final ConfigNodeProcedureEnv env) {
    new TableRegionTaskExecutor<>(
            "delete table device in black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup4TableModel(database),
            CnToDnAsyncRequestType.DELETE_TABLE_DEVICE_IN_BLACK_LIST,
            (dataNodeLocation, consensusGroupIdList) ->
                new TTableDeviceDeletionWithPatternOrModReq(
                    consensusGroupIdList, tableName, ByteBuffer.wrap(patternBytes)))
        .execute();
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
                          new PipeDeleteDevicesPlan(
                              database, tableName, patternBytes, filterBytes, modBytes))
                      : new PipeDeleteDevicesPlan(
                          database, tableName, patternBytes, filterBytes, modBytes));
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
      final ConfigNodeProcedureEnv env, final DeleteDevicesState deleteDevicesState)
      throws IOException, InterruptedException, ProcedureException {
    if (deleteDevicesState == CONSTRUCT_BLACK_LIST) {
      new TableRegionTaskExecutor<>(
              "roll back table device black list",
              env,
              env.getConfigManager().getRelatedSchemaRegionGroup4TableModel(database),
              CnToDnAsyncRequestType.ROLLBACK_TABLE_DEVICE_BLACK_LIST,
              (dataNodeLocation, consensusGroupIdList) ->
                  new TTableDeviceDeletionWithPatternOrModReq(
                      consensusGroupIdList, tableName, ByteBuffer.wrap(patternBytes)))
          .execute();
    }
  }

  public long getDeletedDevicesNum() {
    return deletedDevicesNum;
  }

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
    return CHECK_TABLE_EXISTENCE;
  }

  @Override
  protected String getActionMessage() {
    // Not used
    return null;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DELETE_DEVICES_PROCEDURE.getTypeCode()
            : ProcedureType.DELETE_DEVICES_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(patternBytes.length, stream);
    stream.write(patternBytes);
    ReadWriteIOUtils.write(filterBytes.length, stream);
    stream.write(filterBytes);
    ReadWriteIOUtils.write(modBytes.length, stream);
    stream.write(modBytes);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    patternBytes = new byte[ReadWriteIOUtils.readInt(byteBuffer)];
    byteBuffer.get(patternBytes);
    filterBytes = new byte[ReadWriteIOUtils.readInt(byteBuffer)];
    byteBuffer.get(filterBytes);
    modBytes = new byte[ReadWriteIOUtils.readInt(byteBuffer)];
    byteBuffer.get(modBytes);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Arrays.equals(this.patternBytes, ((DeleteDevicesProcedure) o).patternBytes)
        && Arrays.equals(this.filterBytes, ((DeleteDevicesProcedure) o).filterBytes)
        && Arrays.equals(this.modBytes, ((DeleteDevicesProcedure) o).modBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        Arrays.hashCode(patternBytes),
        Arrays.hashCode(filterBytes),
        Arrays.hashCode(modBytes));
  }
}
