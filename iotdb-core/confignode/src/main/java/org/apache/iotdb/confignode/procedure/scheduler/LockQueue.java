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
1import java.util.ArrayDeque;
1
1/** Lock Queue for procedure of the same type */
1public class LockQueue {
1  private final ArrayDeque<Procedure<?>> deque = new ArrayDeque<>();
1
1  private Procedure<?> lockOwnerProcedure = null;
1
1  public boolean tryLock(Procedure<?> procedure) {
1    if (lockOwnerProcedure == null) {
1      lockOwnerProcedure = procedure;
1      return true;
1    }
1    return procedure.getProcId() == lockOwnerProcedure.getProcId();
1  }
1
1  public boolean releaseLock(Procedure<?> procedure) {
1    if (lockOwnerProcedure == null || lockOwnerProcedure.getProcId() != procedure.getProcId()) {
1      return false;
1    }
1    lockOwnerProcedure = null;
1    return true;
1  }
1
1  public void waitProcedure(Procedure<?> procedure) {
1    deque.addLast(procedure);
1  }
1
1  public int wakeWaitingProcedures(ProcedureScheduler procedureScheduler) {
1    int count = deque.size();
1    while (!deque.isEmpty()) {
1      procedureScheduler.addFront(deque.pollFirst());
1    }
1    return count;
1  }
1}
1