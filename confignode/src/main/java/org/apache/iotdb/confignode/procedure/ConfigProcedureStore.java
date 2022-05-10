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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedureReq;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedureReq;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.procedure.Procedure;
import org.apache.iotdb.procedure.store.IProcedureStore;
import org.apache.iotdb.procedure.store.ProcedureStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ConfigProcedureStore implements IProcedureStore {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureStore.class);

  private volatile boolean isRunning = false;
  private ProcedureInfo procedureInfo = ProcedureInfo.getInstance();
  private final String procedureWalDir =
      ConfigNodeDescriptor.getInstance().getConf().getProcedureWalDir();
  private ConfigManager configManager;

  public ConfigProcedureStore() {}

  public ConfigProcedureStore(ConfigManager configManager) {
    this.configManager = configManager;
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
    UpdateProcedureReq updateProcedureReq = new UpdateProcedureReq();
    ProcedureFactory.ProcedureType procedureType = ProcedureFactory.getProcedureType(procedure);
    if (procedureType != null) {
      updateProcedureReq.setProcedure(procedure);
    }
    getConsensusManager().write(updateProcedureReq);
  }

  @Override
  public void update(Procedure[] subprocs) {
    for (Procedure subproc : subprocs) {
      update(subproc);
    }
  }

  @Override
  public void delete(long procId) {
    DeleteProcedureReq deleteProcedureReq = new DeleteProcedureReq();
    deleteProcedureReq.setProcId(procId);
    getConsensusManager().write(deleteProcedureReq);
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
