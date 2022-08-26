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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class TestProcEnv {
  public ProcedureScheduler scheduler;

  private final ReentrantLock envLock = new ReentrantLock();

  private final AtomicInteger acc = new AtomicInteger(0);

  public final AtomicInteger successCount = new AtomicInteger(0);

  public final AtomicInteger rolledBackCount = new AtomicInteger(0);

  public StringBuilder executeSeq = new StringBuilder();

  public StringBuilder lockAcquireSeq = new StringBuilder();

  public AtomicInteger getAcc() {
    return acc;
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public ReentrantLock getEnvLock() {
    return envLock;
  }

  public void setScheduler(ProcedureScheduler scheduler) {
    this.scheduler = scheduler;
  }
}
