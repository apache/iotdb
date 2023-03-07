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

package org.apache.iotdb.confignode.procedure.store;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ConfigProcedureStore implements IProcedureStore {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigProcedureStore.class);

  private volatile boolean isRunning = false;
  private final ProcedureInfo procedureInfo;
  private final String procedureWalDir =
      CommonDescriptor.getInstance().getConfig().getProcedureWalFolder();
  private final ConfigManager configManager;

  public ConfigProcedureStore(ConfigManager configManager, ProcedureInfo procedureInfo) {
    this.configManager = configManager;
    this.procedureInfo = procedureInfo;
    try {
      checkProcWalDir(procedureWalDir);
    } catch (IOException e) {
      LOG.error("ConfigProcedureStore start failed ", e);
    }
  }

  public ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public void setRunning(boolean running) {
    this.isRunning = running;
  }

  @Override
  public void load(List<Procedure> procedureList) {
    procedureInfo.load(procedureList);
  }

  @Override
  public void update(Procedure procedure) {
    UpdateProcedurePlan updateProcedurePlan = new UpdateProcedurePlan();
    ProcedureType procedureType = ProcedureFactory.getProcedureType(procedure);
    if (procedureType != null) {
      updateProcedurePlan.setProcedure(procedure);
    }
    getConsensusManager().write(updateProcedurePlan);
  }

  @Override
  public void update(Procedure[] subprocs) {
    for (Procedure subproc : subprocs) {
      update(subproc);
    }
  }

  @Override
  public void delete(long procId) {
    DeleteProcedurePlan deleteProcedurePlan = new DeleteProcedurePlan();
    deleteProcedurePlan.setProcId(procId);
    getConsensusManager().write(deleteProcedurePlan);
  }

  @Override
  public void delete(long[] childProcIds) {
    for (long childProcId : childProcIds) {
      delete(childProcId);
    }
  }

  @Override
  public void delete(long[] batchIds, int startIndex, int batchCount) {
    for (int i = startIndex; i < batchCount; i++) {
      delete(batchIds[i]);
    }
  }

  /** clean all the wal, used for unit test. */
  public void cleanup() {
    // no op
  }

  public void stop() {
    isRunning = false;
  }

  @Override
  public void start() {
    if (!isRunning) {
      isRunning = true;
    }
  }

  private void checkProcWalDir(String procedureWalDir) throws IOException {
    File dir = new File(procedureWalDir);
    if (!dir.exists()) {
      if (dir.mkdirs()) {
        LOG.info("Make procedure wal dir: {}", dir);
      } else {
        throw new IOException(
            String.format(
                "Start ConfigNode failed, because couldn't make system dirs: %s.",
                dir.getAbsolutePath()));
      }
    }
  }
}
