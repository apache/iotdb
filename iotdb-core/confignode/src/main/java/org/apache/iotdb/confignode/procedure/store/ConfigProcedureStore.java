1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.store;
1
1import org.apache.iotdb.commons.conf.CommonDescriptor;
1import org.apache.iotdb.commons.utils.FileUtils;
1import org.apache.iotdb.commons.utils.TestOnly;
1import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
1import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
1import org.apache.iotdb.confignode.manager.ConfigManager;
1import org.apache.iotdb.confignode.persistence.ProcedureInfo;
1import org.apache.iotdb.confignode.procedure.Procedure;
1import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
1import org.apache.iotdb.consensus.exception.ConsensusException;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.io.File;
1import java.io.IOException;
1import java.util.List;
1import java.util.Objects;
1
1public class ConfigProcedureStore implements IProcedureStore<ConfigNodeProcedureEnv> {
1
1  private static final Logger LOG = LoggerFactory.getLogger(ConfigProcedureStore.class);
1
1  private volatile boolean isRunning = false;
1  private final ProcedureInfo procedureInfo;
1  private final String procedureWalDir =
1      CommonDescriptor.getInstance().getConfig().getProcedureWalFolder();
1  private final ConfigManager configManager;
1
1  public ConfigProcedureStore(ConfigManager configManager, ProcedureInfo procedureInfo) {
1    this.configManager = configManager;
1    this.procedureInfo = procedureInfo;
1    try {
1      checkProcWalDir(procedureWalDir);
1    } catch (IOException e) {
1      LOG.error("ConfigProcedureStore start failed ", e);
1    }
1  }
1
1  @Override
1  public boolean isRunning() {
1    return isRunning;
1  }
1
1  @Override
1  public void setRunning(boolean running) {
1    this.isRunning = running;
1  }
1
1  @Override
1  public List<Procedure<ConfigNodeProcedureEnv>> load() {
1    return procedureInfo.oldLoad();
1  }
1
1  @Override
1  public List<Procedure<ConfigNodeProcedureEnv>> getProcedures() {
1    return procedureInfo.getProcedures();
1  }
1
1  @Override
1  public ProcedureInfo getProcedureInfo() {
1    return procedureInfo;
1  }
1
1  @Override
1  public long getNextProcId() {
1    return procedureInfo.getNextProcId();
1  }
1
1  @Override
1  public void update(Procedure<ConfigNodeProcedureEnv> procedure) {
1    Objects.requireNonNull(ProcedureFactory.getProcedureType(procedure), "Procedure type is null");
1    final UpdateProcedurePlan updateProcedurePlan = new UpdateProcedurePlan(procedure);
1    try {
1      configManager.getConsensusManager().write(updateProcedurePlan);
1    } catch (ConsensusException e) {
1      LOG.warn("Failed in the write API executing the consensus layer due to: ", e);
1    }
1  }
1
1  @Override
1  public void update(Procedure[] subprocs) {
1    for (Procedure subproc : subprocs) {
1      update(subproc);
1    }
1  }
1
1  @Override
1  public void delete(long procId) {
1    DeleteProcedurePlan deleteProcedurePlan = new DeleteProcedurePlan();
1    deleteProcedurePlan.setProcId(procId);
1    try {
1      configManager.getConsensusManager().write(deleteProcedurePlan);
1    } catch (ConsensusException e) {
1      LOG.warn("Failed in the write API executing the consensus layer due to: ", e);
1    }
1  }
1
1  @Override
1  public void delete(long[] childProcIds) {
1    for (long childProcId : childProcIds) {
1      delete(childProcId);
1    }
1  }
1
1  @Override
1  public void delete(long[] batchIds, int startIndex, int batchCount) {
1    for (int i = startIndex; i < batchCount; i++) {
1      delete(batchIds[i]);
1    }
1  }
1
1  /** clean all the wal, used for unit test. */
1  public void cleanup() {
1    // no op
1  }
1
1  public void stop() {
1    isRunning = false;
1  }
1
1  @Override
1  public void start() {
1    if (!isRunning) {
1      isRunning = true;
1    }
1  }
1
1  private void checkProcWalDir(String procedureWalDir) throws IOException {
1    File dir = new File(procedureWalDir);
1    checkOldProcWalDir(dir);
1  }
1
1  @TestOnly
1  public static void createOldProcWalDir() throws IOException {
1    File dir = new File(CommonDescriptor.getInstance().getConfig().getProcedureWalFolder());
1    if (!dir.exists()) {
1      if (dir.mkdirs()) {
1        LOG.info("Make procedure wal dir: {}", dir);
1      } else {
1        throw new IOException(
1            String.format(
1                "Start ConfigNode failed, because couldn't make system dirs: %s.",
1                dir.getAbsolutePath()));
1      }
1    }
1  }
1
1  private void checkOldProcWalDir(File newDir) {
1    File oldDir = new File(CommonDescriptor.getInstance().getConfig().getOldProcedureWalFolder());
1    if (oldDir.exists()) {
1      FileUtils.moveFileSafe(oldDir, newDir);
1    }
1  }
1
1  @Override
1  public boolean isOldVersionProcedureStore() {
1    return procedureInfo.isOldVersion();
1  }
1}
1