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
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteColumnDataReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateColumnCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

public class DropTableColumnProcedure
    extends AbstractAlterOrDropTableProcedure<DropTableColumnState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableColumnProcedure.class);

  private String columnName;
  private boolean isAttributeColumn;

  public DropTableColumnProcedure() {
    super();
  }

  public DropTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName) {
    super(database, tableName, queryId);
    this.columnName = columnName;
  }

  @Override
  protected String getActionMessage() {
    return "drop table column";
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DropTableColumnState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_COLUMN:
          LOGGER.info(
              "Check and invalidate column {} in {}.{} when dropping column",
              columnName,
              database,
              tableName);
          checkAndPreDeleteColumn(env);
          break;
        case INVALIDATE_CACHE:
          LOGGER.info(
              "Invalidating cache for column {} in {}.{} when dropping column",
              columnName,
              database,
              tableName);
          invalidateCache(env);
          break;
        case EXECUTE_ON_REGIONS:
          LOGGER.info(
              "Executing on region for column {} in {}.{} when dropping column",
              columnName,
              database,
              tableName);
          executeOnRegions(env);
          break;
        case DROP_COLUMN:
          LOGGER.info("Dropping column {} in {}.{} on configNode", columnName, database, tableName);
          dropColumn(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized CreateTableState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "DropTableColumn-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAndPreDeleteColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new PreDeleteColumnPlan(database, tableName, columnName), env, LOGGER);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      isAttributeColumn = status.isSetMessage();
      setNextState(DropTableColumnState.INVALIDATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TInvalidateColumnCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_COLUMN_CACHE,
            new TInvalidateColumnCacheReq(database, tableName, columnName, isAttributeColumn),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // All dataNodes must clear the related schemaEngine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to invalidate {} column {}'s cache of table {}.{}",
            isAttributeColumn ? "attribute" : "measurement",
            columnName,
            database,
            tableName);
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        "Invalidate column %s cache failed for table %s.%s",
                        columnName, database, tableName))));
        return;
      }
    }

    setNextState(DropTableColumnState.EXECUTE_ON_REGIONS);
  }

  private void executeOnRegions(final ConfigNodeProcedureEnv env) {
    final PathPatternTree patternTree = new PathPatternTree();
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

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedRegionGroup =
        isAttributeColumn
            ? env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, true)
            : env.getConfigManager().getRelatedDataRegionGroup(patternTree, true);

    if (!relatedRegionGroup.isEmpty()) {
      new TableRegionTaskExecutor<>(
              "delete data for drop table",
              env,
              relatedRegionGroup,
              CnToDnAsyncRequestType.DELETE_COLUMN_DATA,
              ((dataNodeLocation, consensusGroupIdList) ->
                  new TDeleteColumnDataReq(
                      new ArrayList<>(consensusGroupIdList),
                      tableName,
                      columnName,
                      isAttributeColumn)))
          .execute();
    }

    setNextState(DropTableColumnState.DROP_COLUMN);
  }

  private void dropColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new CommitDeleteColumnPlan(database, tableName, columnName), env, LOGGER);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected boolean isRollbackSupported(final DropTableColumnState state) {
    return false;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv,
      final DropTableColumnState dropTableState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }

  @Override
  protected DropTableColumnState getState(final int stateId) {
    return DropTableColumnState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropTableColumnState dropTableColumnState) {
    return dropTableColumnState.ordinal();
  }

  @Override
  protected DropTableColumnState getInitialState() {
    return DropTableColumnState.CHECK_AND_INVALIDATE_COLUMN;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_TABLE_COLUMN_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(columnName, stream);
    ReadWriteIOUtils.write(isAttributeColumn, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    this.columnName = ReadWriteIOUtils.readString(byteBuffer);
    this.isAttributeColumn = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(columnName, ((DropTableColumnProcedure) o).columnName)
        && Objects.equals(isAttributeColumn, ((DropTableColumnProcedure) o).isAttributeColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), columnName, isAttributeColumn);
  }
}
