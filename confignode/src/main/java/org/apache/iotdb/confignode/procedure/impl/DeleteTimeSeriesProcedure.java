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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.confignode.procedure.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.DeleteTimeSeriesState;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private static final int retryThreshold = 5;

  private static Map<Long, PathPatternTree> RUNNING_TASKS = new ConcurrentHashMap<>();

  private PathPatternTree patternTree;

  public static boolean checkIsOverlapWithRunningTasks(PathPatternTree patternTree) {
    for (PathPatternTree existingPatternTree : RUNNING_TASKS.values()) {
      if (patternTree.isOverlapWith(existingPatternTree)) {
        return false;
      }
    }
    return true;
  }

  public DeleteTimeSeriesProcedure() {}

  public DeleteTimeSeriesProcedure(PathPatternTree patternTree) {
    this.patternTree = patternTree;
  }

  public static void putRunningTask(long procId, PathPatternTree patternTree) {
    RUNNING_TASKS.put(procId, patternTree);
  }

  public static void removeRunningTask(long procId) {
    RUNNING_TASKS.remove(procId);
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DeleteTimeSeriesState deleteTimeSeriesState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DeleteTimeSeriesState deleteTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DeleteTimeSeriesState getState(int stateId) {
    return null;
  }

  @Override
  protected int getStateId(DeleteTimeSeriesState deleteTimeSeriesState) {
    return 0;
  }

  @Override
  protected DeleteTimeSeriesState getInitialState() {
    return null;
  }
}
