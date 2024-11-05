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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TConstructTableDeviceBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackOrDeleteTableDeviceInBlackListReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.CLEAN_DATANODE_SCHEMA_CACHE;
import static org.apache.iotdb.confignode.procedure.state.schema.DeleteDevicesState.CONSTRUCT_BLACK_LIST;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_ALREADY_EXISTS;

public class DeleteDevicesProcedure extends AbstractAlterOrDropTableProcedure<DeleteDevicesState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteDevicesProcedure.class);
  private byte[] patternBytes;
  private byte[] filterBytes;

  // Transient
  private PathPatternTree patternTree;

  // Transient, will not be returned if once recovers
  private long deletedDevicesNum;

  public DeleteDevicesProcedure() {
    super();
  }

  public DeleteDevicesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final byte[] patternBytes,
      final byte[] filterBytes) {
    super(database, tableName, queryId);
    this.patternBytes = patternBytes;
    this.filterBytes = filterBytes;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DeleteDevicesState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_TABLE_EXISTENCE:
          LOGGER.info("Check the existence of table {}.{}", database, table.getTableName());
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
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "DeleteTimeSeries-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void checkTableExistence(final ConfigNodeProcedureEnv env) {
    if (env.getConfigManager()
        .getClusterSchemaManager()
        .getTableIfExists(database, table.getTableName())
        .isPresent()) {
      setFailure(
          new ProcedureException(
              new IoTDBException(
                  String.format(
                      "Table '%s.%s' already exists.",
                      database.substring(ROOT.length() + 1), table.getTableName()),
                  TABLE_ALREADY_EXISTS.getStatusCode())));
    } else {
      setNextState(CONSTRUCT_BLACK_LIST);
    }
  }

  private void constructBlackList(final ConfigNodeProcedureEnv env) {
    patternTree = new PathPatternTree();
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    final PartialPath path;
    try {
      path = new PartialPath(new String[] {ROOT, database.substring(5), tableName});
      patternTree.appendPathPattern(path);
      patternTree.appendPathPattern(path.concatAsMeasurementPath(MULTI_LEVEL_PATH_WILDCARD));
      patternTree.serialize(dataOutputStream);
    } catch (final IOException e) {
      LOGGER.warn("failed to serialize request for table {}.{}", database, table.getTableName(), e);
    }

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);

    if (relatedSchemaRegionGroup.isEmpty()) {
      deletedDevicesNum = 0;
    }
    final List<TSStatus> successResult = new ArrayList<>();
    new DataNodeRegionTaskExecutor<TConstructTableDeviceBlackListReq, TSStatus>(
        env,
        relatedSchemaRegionGroup,
        false,
        CnToDnAsyncRequestType.CONSTRUCT_TABLE_DEVICE_BLACK_LIST,
        ((dataNodeLocation, consensusGroupIdList) ->
            new TConstructTableDeviceBlackListReq(
                new ArrayList<>(consensusGroupIdList),
                tableName,
                ByteBuffer.wrap(patternBytes),
                ByteBuffer.wrap(filterBytes)))) {
      @Override
      protected List<TConsensusGroupId> processResponseOfOneDataNode(
          final TDataNodeLocation dataNodeLocation,
          final List<TConsensusGroupId> consensusGroupIdList,
          final TSStatus response) {
        final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
        if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          successResult.add(response);
        } else if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          final List<TSStatus> subStatusList = response.getSubStatus();
          for (int i = 0; i < subStatusList.size(); i++) {
            if (subStatusList.get(i).getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              successResult.add(subStatusList.get(i));
            } else {
              failedRegionList.add(consensusGroupIdList.get(i));
            }
          }
        } else {
          failedRegionList.addAll(consensusGroupIdList);
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
                        "[%s] for %s.%s failed when construct black list for table because failed to execute in all replicaset of %s %s. Failure nodes: %s",
                        this.getClass().getSimpleName(),
                        database,
                        tableName,
                        consensusGroupId.type,
                        consensusGroupId.id,
                        dataNodeLocationSet))));
        interruptTask();
      }
    }.execute();

    setNextState(CONSTRUCT_BLACK_LIST);
    deletedDevicesNum =
        !isFailed()
            ? successResult.stream()
                .mapToLong(resp -> Long.parseLong(resp.getMessage()))
                .reduce(Long::sum)
                .orElse(0L)
            : 0;
  }

  private void deleteDeviceSchema(final ConfigNodeProcedureEnv env) {
    new TableRegionTaskExecutor<>(
            "roll back table device black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
            CnToDnAsyncRequestType.DELETE_TABLE_DEVICE_IN_BLACK_LIST,
            (dataNodeLocation, consensusGroupIdList) ->
                new TRollbackOrDeleteTableDeviceInBlackListReq(
                    consensusGroupIdList, tableName, ByteBuffer.wrap(filterBytes)))
        .execute();
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final DeleteDevicesState deleteDevicesState)
      throws IOException, InterruptedException, ProcedureException {
    if (deleteDevicesState == CONSTRUCT_BLACK_LIST) {
      new TableRegionTaskExecutor<>(
              "roll back table device black list",
              env,
              env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
              CnToDnAsyncRequestType.ROLLBACK_TABLE_DEVICE_BLACK_LIST,
              (dataNodeLocation, consensusGroupIdList) ->
                  new TRollbackOrDeleteTableDeviceInBlackListReq(
                      consensusGroupIdList, tableName, ByteBuffer.wrap(filterBytes)))
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
    return CONSTRUCT_BLACK_LIST;
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

    ReadWriteIOUtils.write(patternBytes.length, stream);
    stream.write(patternBytes);
    ReadWriteIOUtils.write(filterBytes.length, stream);
    stream.write(filterBytes);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    patternBytes = new byte[ReadWriteIOUtils.readInt(byteBuffer)];
    byteBuffer.get(patternBytes);
    filterBytes = new byte[ReadWriteIOUtils.readInt(byteBuffer)];
    byteBuffer.get(filterBytes);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Arrays.equals(this.patternBytes, ((DeleteDevicesProcedure) o).patternBytes)
        && Arrays.equals(this.filterBytes, ((DeleteDevicesProcedure) o).filterBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), Arrays.hashCode(patternBytes), Arrays.hashCode(filterBytes));
  }
}
