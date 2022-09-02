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
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private static final int RETRY_THRESHOLD = 5;

  private PathPatternTree patternTree;

  public DeleteTimeSeriesProcedure() {
    super();
  }

  public DeleteTimeSeriesProcedure(PathPatternTree patternTree) {
    super();
    this.patternTree = patternTree;
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DeleteTimeSeriesState deleteTimeSeriesState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  private void constructBlackList(ConfigNodeProcedureEnv configNodeProcedureEnv) {}

  private void invalidateCache(ConfigNodeProcedureEnv configNodeProcedureEnv) {}

  private void DeleteData(ConfigNodeProcedureEnv configNodeProcedureEnv) {}

  private void DeleteTimeSeries(ConfigNodeProcedureEnv configNodeProcedureEnv) {}

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DeleteTimeSeriesState deleteTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {}

  private void rollbackBlackList() {}

  @Override
  protected boolean isRollbackSupported(DeleteTimeSeriesState deleteTimeSeriesState) {
    return true;
  }

  @Override
  protected DeleteTimeSeriesState getState(int stateId) {
    return DeleteTimeSeriesState.values()[stateId];
  }

  @Override
  protected int getStateId(DeleteTimeSeriesState deleteTimeSeriesState) {
    return deleteTimeSeriesState.ordinal();
  }

  @Override
  protected DeleteTimeSeriesState getInitialState() {
    return DeleteTimeSeriesState.CONSTRUCT_BLACK_LIST;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.DELETE_TIMESERIES_PROCEDURE.ordinal());
    super.serialize(stream);
    patternTree.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    patternTree = PathPatternTree.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeleteTimeSeriesProcedure that = (DeleteTimeSeriesProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getState() == that.getState()
        && patternTree.equals(that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), patternTree);
  }
}
