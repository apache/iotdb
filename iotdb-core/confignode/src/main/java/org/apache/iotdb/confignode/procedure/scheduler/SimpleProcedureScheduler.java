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
1package org.apache.iotdb.confignode.procedure.scheduler;
1
1import org.apache.iotdb.confignode.procedure.Procedure;
1
1import java.util.concurrent.LinkedBlockingDeque;
1
1/** Simple scheduler for procedures */
1public class SimpleProcedureScheduler extends AbstractProcedureScheduler {
1  // Use "LinkedBlockingDeque" to ensure thread-safety
1  private final LinkedBlockingDeque<Procedure> runnables = new LinkedBlockingDeque<>();
1  private final LinkedBlockingDeque<Procedure> waitings = new LinkedBlockingDeque<>();
1
1  @Override
1  protected void enqueue(final Procedure procedure, final boolean addFront) {
1    if (addFront) {
1      runnables.addFirst(procedure);
1    } else {
1      runnables.addLast(procedure);
1    }
1  }
1
1  @Override
1  protected Procedure dequeue() {
1    return runnables.poll();
1  }
1
1  @Override
1  public void clear() {
1    schedLock();
1    try {
1      runnables.clear();
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  @Override
1  public void yield(final Procedure proc) {
1    addBack(proc);
1  }
1
1  @Override
1  public boolean queueHasRunnables() {
1    return !runnables.isEmpty();
1  }
1
1  @Override
1  public int queueSize() {
1    return runnables.size();
1  }
1
1  public void addWaiting(Procedure proc) {
1    waitings.add(proc);
1  }
1
1  public void releaseWaiting() {
1    runnables.addAll(waitings);
1    waitings.clear();
1  }
1}
1