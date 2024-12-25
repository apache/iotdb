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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureWAL;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class ProcedureInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureInfo.class);

  private static final String MAIN_SNAPSHOT_FILENAME = "procedure_info.bin";
  private static final String PROCEDURE_SNAPSHOT_DIR = "procedures";
  private static final String PROCEDURE_SNAPSHOT_FILE_SUFFIX = ".bin";
  private static final int PROCEDURE_LOAD_BUFFER_SIZE = 8 * 1024 * 1024;
  private static final String PROCEDURE_WAL_SUFFIX = ".proc.wal";
  private final String OLD_PROCEDURE_WAL_DIR =
      CommonDescriptor.getInstance().getConfig().getProcedureWalFolder();

  private final Map<Long, Procedure<ConfigNodeProcedureEnv>> procedureMap =
      new ConcurrentHashMap<>();

  private final AtomicLong lastProcId = new AtomicLong(-1);

  private final ProcedureFactory procedureFactory = ProcedureFactory.getInstance();

  private final ConfigManager configManager;

  public ProcedureInfo(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public boolean isOldVersion() {
    return new File(OLD_PROCEDURE_WAL_DIR).exists();
  }

  public List<Procedure<ConfigNodeProcedureEnv>> oldLoad() {
    List<Procedure<ConfigNodeProcedureEnv>> procedureList = new ArrayList<>();
    try (Stream<Path> s = Files.list(Paths.get(OLD_PROCEDURE_WAL_DIR))) {
      s.filter(path -> path.getFileName().toString().endsWith(PROCEDURE_WAL_SUFFIX))
          .sorted(
              (p1, p2) ->
                  Long.compareUnsigned(
                      Long.parseLong(p1.getFileName().toString().split("\\.")[0]),
                      Long.parseLong(p2.getFileName().toString().split("\\.")[0])))
          .forEach(path -> loadProcedure(path).ifPresent(procedureList::add));
    } catch (IOException e) {
      LOGGER.error("Load procedure wal failed.", e);
    }
    procedureList.forEach(procedure -> procedureMap.put(procedure.getProcId(), procedure));
    procedureList.forEach(
        procedure -> lastProcId.set(Math.max(lastProcId.get(), procedure.getProcId())));
    return procedureList;
  }

  public void upgrade() {
    if (isOldVersion()) {
      try {
        LOGGER.info("Old procedure files have been loaded successfully, taking snapshot...");
        configManager.getConsensusManager().manuallyTakeSnapshot();
      } catch (ConsensusException e) {
        LOGGER.warn("Taking snapshot fail, procedure upgrade fail", e);
        return;
      }
      try {
        FileUtils.recursivelyDeleteFolder(OLD_PROCEDURE_WAL_DIR);
      } catch (IOException e) {
        LOGGER.error("Delete useless procedure wal dir fail.", e);
        LOGGER.error(
            "You should manually delete the procedure wal dir before ConfigNode restart. {}",
            OLD_PROCEDURE_WAL_DIR);
      }
      LOGGER.info(
          "The Procedure framework has been successfully upgraded. Now it uses the consensus layer's services instead of maintaining the WAL itself.");
    }
  }

  public TSStatus updateProcedure(UpdateProcedurePlan updateProcedurePlan) {
    Procedure<ConfigNodeProcedureEnv> procedure = updateProcedurePlan.getProcedure();
    procedureMap.put(procedure.getProcId(), procedure);
    lastProcId.updateAndGet(id -> Math.max(id, procedure.getProcId()));
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @TestOnly
  public TSStatus oldUpdateProcedure(UpdateProcedurePlan updateProcedurePlan) {
    Procedure procedure = updateProcedurePlan.getProcedure();
    long procId = procedure.getProcId();
    Path path = Paths.get(OLD_PROCEDURE_WAL_DIR, procId + PROCEDURE_WAL_SUFFIX);
    ProcedureWAL procedureWAL = new ProcedureWAL(path, procedureFactory);
    try {
      procedureWAL.save(procedure);
    } catch (IOException e) {
      LOGGER.error("Update Procedure (pid={}) wal failed", procedure.getProcId(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus deleteProcedure(DeleteProcedurePlan deleteProcedurePlan) {
    procedureMap.remove(deleteProcedurePlan.getProcId());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static Optional<Procedure> loadProcedure(Path procedureFilePath) {
    try (FileInputStream fis = new FileInputStream(procedureFilePath.toFile())) {
      Procedure procedure = null;
      try (FileChannel channel = fis.getChannel()) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(PROCEDURE_LOAD_BUFFER_SIZE);
        if (channel.read(byteBuffer) > 0) {
          byteBuffer.flip();
          procedure = ProcedureFactory.getInstance().create(byteBuffer);
          byteBuffer.clear();
        }
        return Optional.ofNullable(procedure);
      }
    } catch (Exception e) {
      LOGGER.error("Load {} failed, it will be deleted.", procedureFilePath, e);
      if (!procedureFilePath.toFile().delete()) {
        LOGGER.error("{} deleted failed; take appropriate action.", procedureFilePath, e);
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File procedureSnapshotDir = new File(snapshotDir, PROCEDURE_SNAPSHOT_DIR);
    if (procedureSnapshotDir.exists()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot dir [{}] is already exist.",
          procedureSnapshotDir.getAbsolutePath());
      return false;
    }
    File tmpDir = new File(procedureSnapshotDir.getAbsolutePath() + "-" + UUID.randomUUID());
    if (!tmpDir.mkdir()) {
      LOGGER.error("Failed to take snapshot, because create tmp dir [{}] fail.", tmpDir);
      return false;
    }

    // save lastProcId
    File mainFile = new File(tmpDir.getAbsolutePath() + File.separator + MAIN_SNAPSHOT_FILENAME);
    try (FileOutputStream fileOutputStream = new FileOutputStream(mainFile);
        DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {
      ReadWriteIOUtils.write(lastProcId.get(), fileOutputStream);
      tioStreamTransport.flush();
      fileOutputStream.getFD().sync();
    }

    // save all procedures
    AtomicBoolean snapshotAllSuccess = new AtomicBoolean(true);
    procedureMap
        .values()
        .forEach(
            procedure -> {
              try {
                new ProcedureWAL(
                        Paths.get(
                            tmpDir.getAbsolutePath()
                                + File.separator
                                + procedure.getProcId()
                                + PROCEDURE_SNAPSHOT_FILE_SUFFIX),
                        procedureFactory)
                    .save(procedure);
              } catch (IOException e) {
                snapshotAllSuccess.set(false);
                LOGGER.warn(
                    "{} id {} took snapshot fail", procedure.getClass(), procedure.getProcId(), e);
              }
            });
    if (!snapshotAllSuccess.get()) {
      return false;
    }

    return tmpDir.renameTo(procedureSnapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File procedureSnapshotDir = new File(snapshotDir, PROCEDURE_SNAPSHOT_DIR);
    if (!procedureSnapshotDir.exists() || !procedureSnapshotDir.isDirectory()) {
      LOGGER.error(
          "Failed to load snapshot, because snapshot dir [{}] not exists.",
          procedureSnapshotDir.getAbsolutePath());
      return;
    }

    File mainFile =
        new File(procedureSnapshotDir.getAbsolutePath() + File.separator + MAIN_SNAPSHOT_FILENAME);
    try (FileInputStream fileInputStream = new FileInputStream(mainFile)) {
      lastProcId.set(ReadWriteIOUtils.readLong(fileInputStream));
    }

    Arrays.stream(Objects.requireNonNull(procedureSnapshotDir.listFiles()))
        .forEach(
            procedureSnapshotFile -> {
              if (!procedureSnapshotFile.getName().equals(MAIN_SNAPSHOT_FILENAME)) {
                loadProcedure(procedureSnapshotFile.toPath())
                    .ifPresent(procedure -> procedureMap.put(procedure.getProcId(), procedure));
              }
            });
  }

  public List<Procedure<ConfigNodeProcedureEnv>> getProcedures() {
    return new ArrayList<>(procedureMap.values());
  }

  public long getNextProcId() {
    return this.lastProcId.incrementAndGet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcedureInfo procedureInfo = (ProcedureInfo) o;
    return lastProcId.get() == procedureInfo.lastProcId.get()
        && procedureMap.equals(procedureInfo.procedureMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastProcId, procedureMap);
  }
}
