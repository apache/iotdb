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
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureWAL;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class ProcedureInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureInfo.class);

  private static final String SNAPSHOT_FILENAME = "procedure_info.bin";

  private static final String PROCEDURE_WAL_SUFFIX = ".proc.wal";

  private final Map<Long, Procedure<ConfigNodeProcedureEnv>> procedureMap = new HashMap<>();

  private final AtomicLong lastProcId = new AtomicLong(-1);

  private final ProcedureFactory procedureFactory = ProcedureFactory.getInstance();
  private final String oldProcedureWalDir =
      CommonDescriptor.getInstance().getConfig().getProcedureWalFolder();

  public List<Procedure<ConfigNodeProcedureEnv>> load() {
    List<Procedure<ConfigNodeProcedureEnv>> procedureList = new ArrayList<>();
    try (Stream<Path> s = Files.list(Paths.get(oldProcedureWalDir))) {
      s.filter(path -> path.getFileName().toString().endsWith(PROCEDURE_WAL_SUFFIX))
          .sorted(
              (p1, p2) ->
                  Long.compareUnsigned(
                      Long.parseLong(p1.getFileName().toString().split("\\.")[0]),
                      Long.parseLong(p2.getFileName().toString().split("\\.")[0])))
          .forEach(path -> ProcedureWAL.load(path, procedureFactory).ifPresent(procedureList::add));
    } catch (IOException e) {
      LOGGER.error("Load procedure wal failed.", e);
    }
    return procedureList;
  }

  public TSStatus updateProcedure(UpdateProcedurePlan updateProcedurePlan) {
    Procedure<ConfigNodeProcedureEnv> procedure = updateProcedurePlan.getProcedure();
    procedureMap.put(procedure.getProcId(), procedure);
    long current;
    do {
      current = lastProcId.get();
      if (current >= procedure.getProcId()) {
        break;
      }
    } while (!lastProcId.compareAndSet(current, procedure.getProcId()));
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus deleteProcedure(DeleteProcedurePlan deleteProcedurePlan) {
    procedureMap.remove(deleteProcedurePlan.getProcId());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public long getNextProcId() {
    return this.lastProcId.addAndGet(1);
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {

      ReadWriteIOUtils.write(procedureMap.size(), fileOutputStream);
      for (Procedure<ConfigNodeProcedureEnv> procedure : procedureMap.values()) {
        procedure.serialize(dataOutputStream);
      }

      ReadWriteIOUtils.write(lastProcId.get(), fileOutputStream);

      tioStreamTransport.flush();
      fileOutputStream.getFD().sync();
      tioStreamTransport.close();
      return tmpFile.renameTo(snapshotFile);
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    // TODO: lock ?

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileInputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      long proceduresNum = ReadWriteIOUtils.readLong(fileInputStream);
      for (int i = 0; i < proceduresNum; i++) {
        ProcedureWAL.load(fileInputStream, ProcedureFactory.getInstance())
            .ifPresent(procedure -> procedureMap.put(procedure.getProcId(), procedure));
      }

      ReadWriteIOUtils.readLong(fileInputStream);
    } finally {
      // TODO: unlock ?
    }
  }
}
