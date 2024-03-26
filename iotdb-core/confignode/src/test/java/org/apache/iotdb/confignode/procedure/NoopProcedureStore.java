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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@TestOnly
public class NoopProcedureStore implements IProcedureStore {

  AtomicLong lastProcId = new AtomicLong(-1);

  private volatile boolean running = false;

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void setRunning(boolean running) {
    this.running = running;
  }

  @Override
  public List<Procedure> load() {
    return Collections.emptyList();
  }

  @Override
  public List<Procedure> getProcedures() {
    return Collections.emptyList();
  }

  @Override
  public ProcedureInfo getProcedureInfo() {
    return null;
  }

  @Override
  public synchronized long getNextProcId() {
    return lastProcId.addAndGet(1);
  }

  @Override
  public void update(Procedure procedure) {}

  @Override
  public void update(Procedure[] subprocs) {}

  @Override
  public void delete(long procId) {}

  @Override
  public void delete(long[] childProcIds) {}

  @Override
  public void delete(long[] batchIds, int startIndex, int batchCount) {}

  @Override
  public void cleanup() {}

  @Override
  public void stop() {
    running = false;
  }

  @Override
  public void start() {
    running = true;
  }

  @Override
  public boolean isOldVersionProcedureStore() {
    return true;
  }
}
