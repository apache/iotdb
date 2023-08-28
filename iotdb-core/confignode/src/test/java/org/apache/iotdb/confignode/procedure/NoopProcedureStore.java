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

import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import java.util.List;

public class NoopProcedureStore implements IProcedureStore {

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
  public void load(List<Procedure> procedureList) {}

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
}
