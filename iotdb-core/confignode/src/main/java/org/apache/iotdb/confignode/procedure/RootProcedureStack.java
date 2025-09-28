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
1package org.apache.iotdb.confignode.procedure;
1
1import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
1import org.apache.iotdb.confignode.procedure.state.ProcedureState;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.ArrayList;
1import java.util.HashSet;
1import java.util.List;
1import java.util.Set;
1
1public class RootProcedureStack<Env> {
1  private static final Logger LOG = LoggerFactory.getLogger(RootProcedureStack.class);
1
1  private enum State {
1    RUNNING, // The Procedure is running or ready to run
1    FAILED, // The Procedure failed, waiting for the rollback executing
1    ROLLINGBACK, // The Procedure failed and the execution was rolledback
1  }
1
1  private Set<Procedure<Env>> subprocs = null;
1  private ArrayList<Procedure<Env>> subprocStack = null;
1  private State state = State.RUNNING;
1  private int running = 0;
1
1  public synchronized boolean isFailed() {
1    switch (state) {
1      case ROLLINGBACK:
1      case FAILED:
1        return true;
1      default:
1        break;
1    }
1    return false;
1  }
1
1  public synchronized boolean isRollingback() {
1    return state == State.ROLLINGBACK;
1  }
1
1  /** Called by the {@link ProcedureExecutor} to mark rollback execution. */
1  protected synchronized boolean setRollback() {
1    if (running == 0 && state == State.FAILED) {
1      state = State.ROLLINGBACK;
1      return true;
1    }
1    return false;
1  }
1
1  /** Called by the {@link ProcedureExecutor} to mark rollback execution. */
1  protected synchronized void unsetRollback() {
1    assert state == State.ROLLINGBACK;
1    state = State.FAILED;
1  }
1
1  protected synchronized long[] getSubprocedureIds() {
1    if (subprocs == null) {
1      return new long[0];
1    }
1    return subprocs.stream().mapToLong(Procedure::getProcId).toArray();
1  }
1
1  protected synchronized List<Procedure<Env>> getSubproceduresStack() {
1    return subprocStack;
1  }
1
1  protected synchronized ProcedureException getException() {
1    if (subprocStack != null) {
1      for (Procedure<Env> proc : subprocStack) {
1        if (proc.hasException()) {
1          return proc.getException();
1        }
1      }
1    }
1    return null;
1  }
1
1  /** Called by the {@link ProcedureExecutor} to mark the procedure step as running. */
1  protected synchronized boolean acquire() {
1    if (state != State.RUNNING) {
1      return false;
1    }
1
1    running++;
1    return true;
1  }
1
1  /** Called by the {@link ProcedureExecutor} to mark the procedure step as finished. */
1  protected synchronized void release() {
1    running--;
1  }
1
1  protected synchronized void abort() {
1    if (state == State.RUNNING) {
1      state = State.FAILED;
1    }
1  }
1
1  /**
1   * Called by the {@link ProcedureExecutor} after the procedure step is completed, to add the step
1   * to the rollback list (or procedure stack).
1   */
1  protected synchronized void addRollbackStep(Procedure<Env> proc) {
1    if (proc.isFailed()) {
1      state = State.FAILED;
1    }
1    if (subprocStack == null) {
1      subprocStack = new ArrayList<>();
1    }
1    proc.addStackIndex(subprocStack.size());
1    LOG.trace("Add procedure {} as the {}th rollback step", proc, subprocStack.size());
1    subprocStack.add(proc);
1  }
1
1  protected synchronized void addSubProcedure(Procedure<Env> proc) {
1    if (!proc.hasParent()) {
1      return;
1    }
1    if (subprocs == null) {
1      subprocs = new HashSet<>();
1    }
1    subprocs.add(proc);
1  }
1
1  /**
1   * Called on store load by the {@link ProcedureExecutor} to load part of the stack.
1   *
1   * <p>Each procedure has its own stack-positions. Which means we have to write to the store only
1   * the Procedure we executed, and nothing else. On load we recreate the full stack by aggregating
1   * each procedure stack-positions.
1   */
1  protected synchronized void loadStack(Procedure<Env> proc) {
1    addSubProcedure(proc);
1    int[] stackIndexes = proc.getStackIndexes();
1    if (stackIndexes != null) {
1      if (subprocStack == null) {
1        subprocStack = new ArrayList<>();
1      }
1      int diff = (1 + stackIndexes[stackIndexes.length - 1]) - subprocStack.size();
1      if (diff > 0) {
1        subprocStack.ensureCapacity(1 + stackIndexes[stackIndexes.length - 1]);
1        while (diff-- > 0) {
1          subprocStack.add(null);
1        }
1      }
1      for (int stackIndex : stackIndexes) {
1        subprocStack.set(stackIndex, proc);
1      }
1    }
1    if (proc.getState() == ProcedureState.ROLLEDBACK) {
1      state = State.ROLLINGBACK;
1    } else if (proc.isFailed()) {
1      state = State.FAILED;
1    }
1  }
1}
1