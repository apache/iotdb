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
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class DropTableProcedure extends AbstractAlterOrDropTableProcedure<DropTableState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTableProcedure.class);

  public DropTableProcedure() {}

  public DropTableProcedure(final String database, final String tableName, final String queryId) {
    super(database, tableName, queryId);
  }

  // Not used
  @Override
  protected String getActionMessage() {
    return null;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv configNodeProcedureEnv, final DropTableState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_AND_INVALIDATE_TABLE:
          LOGGER.info("Check and invalidate table {}.{} when dropping table", database, tableName);
          if (!isFailed() && Objects.isNull(table)) {
            LOGGER.info(
                "The updated table has the same properties with the original one. Skip the procedure.");
            return Flow.NO_MORE_STATE;
          }
          break;
        case INVALIDATE_CACHE:
          LOGGER.info(
              "Invalidating cache for table {}.{} when invalidating cache", database, tableName);
          break;
        case DELETE_DATA:
          LOGGER.info("Deleting data for table {}.{}", database, tableName);
          break;
        case DELETE_DEVICES:
          LOGGER.info("Deleting devices for table {}.{} when dropping table", database, tableName);
          return Flow.NO_MORE_STATE;
        case DROP_TABLE:
          LOGGER.info("Dropping table {}.{} on configNode", database, tableName);
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
