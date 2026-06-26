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
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.consensus.exception.ConsensusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ConfigProcedureStore implements IProcedureStore<ConfigNodeProcedureEnv> {

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

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public void setRunning(boolean running) {
    this.isRunning = running;
  }

  @Override
  public List<Procedure<ConfigNodeProcedureEnv>> load() {
    return procedureInfo.oldLoad();
  }

  @Override
  public List<Procedure<ConfigNodeProcedureEnv>> getProcedures() {
    return procedureInfo.getProcedures();
  }

  @Override
  public ProcedureInfo getProcedureInfo() {
    return procedureInfo;
  }

  @Override
  public long getNextProcId() {
    return procedureInfo.getNextProcId();
  }

  @Override
  public void update(Procedure<ConfigNodeProcedureEnv> procedure) throws Exception {
    Objects.requireNonNull(ProcedureFactory.getProcedureType(procedure), "Procedure type is null");
    final UpdateProcedurePlan updateProcedurePlan = new UpdateProcedurePlan(procedure);
    try {
      configManager.getConsensusManager().write(updateProcedurePlan);
    } catch (ConsensusException e) {
      LOG.warn(
          "pid={} Failed in the write update API executing the consensus layer due to: ",
          procedure.getProcId(),
          e);
      // In consensus layer API, do nothing but just throw an exception to let upper caller handle
      // it.
      throw e;
    }
  }

  @Override
  public void update(Procedure[] subprocs) throws Exception {
    for (Procedure subproc : subprocs) {
      update(subproc);
    }
  }

  @Override
  public void delete(long procId) throws Exception {
    DeleteProcedurePlan deleteProcedurePlan = new DeleteProcedurePlan();
    deleteProcedurePlan.setProcId(procId);
    try {
      configManager.getConsensusManager().write(deleteProcedurePlan);
    } catch (ConsensusException e) {
      LOG.warn(
          "pid={} Failed in the write delete API executing the consensus layer due to: ",
          procId,
          e);
      // In consensus layer API, do nothing but just throw an exception to let upper caller handle
      // it.
      throw e;
    }
  }

  @Override
  public void delete(long[] childProcIds) throws Exception {
    for (long childProcId : childProcIds) {
      delete(childProcId);
    }
  }

  @Override
  public void delete(long[] batchIds, int startIndex, int batchCount) throws Exception {
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
    checkOldProcWalDir(dir);
  }

  @TestOnly
  public static void createOldProcWalDir() throws IOException {
    File dir = new File(CommonDescriptor.getInstance().getConfig().getProcedureWalFolder());
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

  private void checkOldProcWalDir(File newDir) {
    File oldDir = new File(CommonDescriptor.getInstance().getConfig().getOldProcedureWalFolder());
    if (oldDir.exists()) {
      FileUtils.moveFileSafe(oldDir, newDir);
    }
  }

  @Override
  public boolean isOldVersionProcedureStore() {
    return procedureInfo.isOldVersion();
  }
}
