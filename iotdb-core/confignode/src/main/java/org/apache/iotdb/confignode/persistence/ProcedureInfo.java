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
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureStore;
import org.apache.iotdb.confignode.procedure.store.ProcedureWAL;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class ProcedureInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureInfo.class);

  private final ProcedureFactory procedureFactory = ProcedureFactory.getInstance();
  private final String procedureWalDir =
      CommonDescriptor.getInstance().getConfig().getProcedureWalFolder();
  private final ConcurrentHashMap<Long, ProcedureWAL> procWALMap = new ConcurrentHashMap<>();

  public void load(List<Procedure> procedureList) {
    try (Stream<Path> s = Files.list(Paths.get(procedureWalDir))) {
      s.filter(path -> path.getFileName().toString().endsWith(ProcedureStore.PROCEDURE_WAL_SUFFIX))
          .sorted(
              (p1, p2) ->
                  Long.compareUnsigned(
                      Long.parseLong(p1.getFileName().toString().split("\\.")[0]),
                      Long.parseLong(p2.getFileName().toString().split("\\.")[0])))
          .forEach(
              path -> {
                String fileName = path.getFileName().toString();
                long procId = Long.parseLong(fileName.split("\\.")[0]);
                ProcedureWAL procedureWAL =
                    procWALMap.computeIfAbsent(
                        procId, id -> new ProcedureWAL(path, procedureFactory));
                procedureWAL.load(procedureList);
              });
    } catch (IOException e) {
      LOG.error("Load procedure wal failed.", e);
    }
  }

  public TSStatus updateProcedure(UpdateProcedurePlan updateProcedurePlan) {
    Procedure procedure = updateProcedurePlan.getProcedure();
    long procId = procedure.getProcId();
    Path path = Paths.get(procedureWalDir, procId + ProcedureStore.PROCEDURE_WAL_SUFFIX);
    ProcedureWAL procedureWAL =
        procWALMap.computeIfAbsent(procId, id -> new ProcedureWAL(path, procedureFactory));
    try {
      procedureWAL.save(procedure);
    } catch (IOException e) {
      LOG.error("Update Procedure (pid={}) wal failed", procedure.getProcId(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus deleteProcedure(DeleteProcedurePlan deleteProcedurePlan) {
    long procId = deleteProcedurePlan.getProcId();
    ProcedureWAL procedureWAL = procWALMap.get(procId);
    if (procedureWAL != null) {
      procedureWAL.delete();
    }
    procWALMap.remove(procId);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
