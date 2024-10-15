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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.confignode.consensus.request.write.table.DropTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DropTableProcedure extends AbstractAlterOrDropTableProcedure<DropTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableProcedure.class);

  public DropTableProcedure() {
    super();
  }

  public DropTableProcedure(final String database, final String tableName, final String queryId) {
    super(database, tableName, queryId);
  }

  // Not used
  @Override
  protected String getActionMessage() {
    return null;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DropTableState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_TABLE:
          LOGGER.info("Check and invalidate table {}.{} when dropping table", database, tableName);
          checkAndPreDeleteTable(env);
          break;
        case INVALIDATE_CACHE:
          LOGGER.info(
              "Invalidating cache for table {}.{} when invalidating cache", database, tableName);
          break;
        case DELETE_DATA:
          LOGGER.info("Deleting data for table {}.{}", database, tableName);
          deleteData(env);
          break;
        case DELETE_DEVICES:
          LOGGER.info("Deleting devices for table {}.{} when dropping table", database, tableName);
          return Flow.NO_MORE_STATE;
        case DROP_TABLE:
          LOGGER.info("Dropping table {}.{} on configNode", database, tableName);
          dropTable(env);
          break;
        default:
          setFailure(new ProcedureException("Unrecognized DropTableState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "DropTable-{}.{}-{} costs {}ms",
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void checkAndPreDeleteTable(final ConfigNodeProcedureEnv env) {
    final PreDeleteTablePlan plan = new PreDeleteTablePlan(database, tableName);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(DropTableState.INVALIDATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void deleteData(final ConfigNodeProcedureEnv env) {
    // TODO
  }

  private void dropTable(final ConfigNodeProcedureEnv env) {
    final DropTablePlan plan = new DropTablePlan(database, tableName);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected boolean isRollbackSupported(final DropTableState state) {
    return false;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv, final DropTableState dropTableState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }

  @Override
  protected DropTableState getState(final int stateId) {
    return DropTableState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropTableState dropTableState) {
    return dropTableState.ordinal();
  }

  @Override
  protected DropTableState getInitialState() {
    return DropTableState.CHECK_AND_INVALIDATE_TABLE;
  }
}
