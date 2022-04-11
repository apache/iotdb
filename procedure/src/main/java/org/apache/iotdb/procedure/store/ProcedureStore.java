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

package org.apache.iotdb.procedure.store;

import org.apache.iotdb.procedure.Procedure;
import org.apache.iotdb.procedure.conf.ProcedureNodeConfigDescriptor;
import org.apache.iotdb.procedure.conf.ProcedureNodeConstant;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ProcedureStore implements IProcedureStore {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureStore.class);
  private static final String PROCEDURE_WAL_DIR =
      ProcedureNodeConfigDescriptor.getInstance().getConf().getProcedureWalDir();
  private static final int PROCEDURE_WAL_BUFFER_SIZE = 16 * 1024 * 1024;
  private final ConcurrentHashMap<Long, ProcedureWAL> procWALMap = new ConcurrentHashMap<>();
  private volatile boolean isRunning = false;

  private ProcedureStore() {
    try {
      Files.createDirectories(Paths.get(PROCEDURE_WAL_DIR));
    } catch (IOException e) {
      throw new RuntimeException("Create procedure wal directory failed.", e);
    }
  }

  public static ProcedureStore getInstance() {
    return ProcedureStoreHolder.INSTANCE;
  }

  public boolean isRunning() {
    return this.isRunning;
  }

  public void setRunning(boolean running) {
    isRunning = running;
  }

  /**
   * Load procedure wal files into memory.
   *
   * @param procedureList procedureList
   */
  public void load(List<Procedure> procedureList) {
    try {
      Files.list(Paths.get(PROCEDURE_WAL_DIR))
          .forEach(
              path -> {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(ProcedureNodeConstant.PROCEDURE_WAL_SUFFIX)) {
                  long procId = Long.parseLong(fileName.split("\\.")[0]);
                  ProcedureWAL procedureWAL =
                      procWALMap.computeIfAbsent(
                          procId,
                          id ->
                              new ProcedureWAL(
                                  path, ByteBuffer.allocate(PROCEDURE_WAL_BUFFER_SIZE)));
                  procedureWAL.load(procedureList);
                }
              });

    } catch (IOException e) {
      LOG.error("Load procedure wal failed.", e);
    }
  }

  /**
   * Update procedure, roughly delete and create a new wal file.
   *
   * @param procedure procedure
   */
  public void update(Procedure procedure) {
    if (!procedure.needPersistance()) {
      procWALMap.remove(procedure.getProcId());
      return;
    }
    long procId = procedure.getProcId();
    Path path = Paths.get(PROCEDURE_WAL_DIR, procId + ProcedureNodeConstant.PROCEDURE_WAL_SUFFIX);
    ProcedureWAL procedureWAL =
        procWALMap.computeIfAbsent(
            procId, id -> new ProcedureWAL(path, ByteBuffer.allocate(PROCEDURE_WAL_BUFFER_SIZE)));
    try {
      ByteBuffer byteBuffer = procedureWAL.getByteBuffer();
      byteBuffer.clear();
      procedure.serialize(byteBuffer);
      procedureWAL.save();
    } catch (IOException e) {
      LOG.error("Update Procedure (pid={}) wal failed", procedure.getProcId());
    }
  }

  /**
   * Batch update
   *
   * @param subprocs procedure array
   */
  public void update(Procedure[] subprocs) {
    for (Procedure subproc : subprocs) {
      update(subproc);
    }
  }

  /**
   * Delete procedure wal file
   *
   * @param procId procedure id
   */
  public void delete(long procId) {
    ProcedureWAL procedureWAL = procWALMap.get(procId);
    if (procedureWAL != null) {
      procedureWAL.delete();
    }
    procWALMap.remove(procId);
  }

  /**
   * Batch delete
   *
   * @param childProcIds procedure id array
   */
  public void delete(long[] childProcIds) {
    for (long childProcId : childProcIds) {
      delete(childProcId);
    }
  }

  /**
   * Batch delete by index
   *
   * @param batchIds batchIds
   * @param startIndex start index
   * @param batchCount delete procedure count
   */
  public void delete(long[] batchIds, int startIndex, int batchCount) {
    for (int i = startIndex; i < batchCount; i++) {
      delete(batchIds[i]);
    }
  }

  /** clean all the wal, used for unit test. */
  public void cleanup() {
    try {
      FileUtils.cleanDirectory(new File(PROCEDURE_WAL_DIR));
    } catch (IOException e) {
      LOG.error("Clean wal directory failed", e);
    }
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

  public static class ProcedureStoreHolder {
    private static final ProcedureStore INSTANCE = new ProcedureStore();
  }
}
