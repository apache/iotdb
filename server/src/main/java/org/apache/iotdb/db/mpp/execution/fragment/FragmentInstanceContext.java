/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.fragment;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.query.context.QueryContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class FragmentInstanceContext extends QueryContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstanceContext.class);
  private static final long END_TIME_INITIAL_VALUE = -1L;
  private final FragmentInstanceId id;

  // TODO if we split one fragment instance into multiple pipelines to run, we need to replace it
  // with CopyOnWriteArrayList or some other thread safe data structure
  private final List<OperatorContext> operatorContexts = new ArrayList<>();

  private DriverContext driverContext;

  private final FragmentInstanceStateMachine stateMachine;

  private final long createNanos = System.nanoTime();

  private final AtomicLong startNanos = new AtomicLong();
  private final AtomicLong endNanos = new AtomicLong();

  private final AtomicReference<Long> executionStartTime = new AtomicReference<>();
  private final AtomicReference<Long> lastExecutionStartTime = new AtomicReference<>();
  private final AtomicReference<Long> executionEndTime = new AtomicReference<>();

  // session info
  private SessionInfo sessionInfo;

  private ExecutorService intoOperationExecutor;

  //    private final GcMonitor gcMonitor;
  //    private final AtomicLong startNanos = new AtomicLong();
  //    private final AtomicLong startFullGcCount = new AtomicLong(-1);
  //    private final AtomicLong startFullGcTimeNanos = new AtomicLong(-1);
  //    private final AtomicLong endNanos = new AtomicLong();
  //    private final AtomicLong endFullGcCount = new AtomicLong(-1);
  //    private final AtomicLong endFullGcTimeNanos = new AtomicLong(-1);

  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      ExecutorService intoOperationExecutor) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(id, stateMachine, sessionInfo, intoOperationExecutor);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  public static FragmentInstanceContext createFragmentInstanceContextForCompaction(long queryId) {
    return new FragmentInstanceContext(queryId);
  }

  private FragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      ExecutorService intoOperationExecutor) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.intoOperationExecutor = intoOperationExecutor;
  }

  @TestOnly
  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(
            id, stateMachine, new SessionInfo(1, "test", ZoneId.systemDefault().getId()), null);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  // used for compaction
  private FragmentInstanceContext(long queryId) {
    this.queryId = queryId;
    this.id = null;
    this.stateMachine = null;
  }

  public void start() {
    long now = System.currentTimeMillis();
    executionStartTime.compareAndSet(null, now);
    startNanos.compareAndSet(0, System.nanoTime());

    // always update last execution start time
    lastExecutionStartTime.set(now);
  }

  // the state change listener is added here in a separate initialize() method
  // instead of the constructor to prevent leaking the "this" reference to
  // another thread, which will cause unsafe publication of this instance.
  private void initialize() {
    stateMachine.addStateChangeListener(this::updateStatsIfDone);
  }

  private void updateStatsIfDone(FragmentInstanceState newState) {
    if (newState.isDone()) {
      long now = System.currentTimeMillis();

      // before setting the end times, make sure a start has been recorded
      executionStartTime.compareAndSet(null, now);
      startNanos.compareAndSet(0, System.nanoTime());

      // Only update last start time, if the nothing was started
      lastExecutionStartTime.compareAndSet(null, now);

      // use compare and set from initial value to avoid overwriting if there
      // were a duplicate notification, which shouldn't happen
      executionEndTime.compareAndSet(END_TIME_INITIAL_VALUE, now);
      endNanos.compareAndSet(0, System.nanoTime());
    }
  }

  public OperatorContext addOperatorContext(
      int operatorId, PlanNodeId planNodeId, String operatorType) {
    checkArgument(operatorId >= 0, "operatorId is negative");

    for (OperatorContext operatorContext : operatorContexts) {
      checkArgument(
          operatorId != operatorContext.getOperatorId(),
          "A context already exists for operatorId %s",
          operatorId);
    }

    OperatorContext operatorContext =
        new OperatorContext(operatorId, planNodeId, operatorType, this);
    operatorContexts.add(operatorContext);
    return operatorContext;
  }

  public List<OperatorContext> getOperatorContexts() {
    return operatorContexts;
  }

  public FragmentInstanceId getId() {
    return id;
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public void setDriverContext(DriverContext driverContext) {
    this.driverContext = driverContext;
  }

  public void failed(Throwable cause) {
    stateMachine.failed(cause);
  }

  /** @return Message string of all failures */
  public String getFailedCause() {
    return stateMachine.getFailureCauses().stream()
        .findFirst()
        .map(Throwable::getMessage)
        .orElse("");
  }

  /** @return List of specific throwable and stack trace */
  public List<FragmentInstanceFailureInfo> getFailureInfoList() {
    return stateMachine.getFailureCauses().stream()
        .map(FragmentInstanceFailureInfo::toFragmentInstanceFailureInfo)
        .collect(Collectors.toList());
  }

  public void finished() {
    stateMachine.finished();
  }

  public void transitionToFlushing() {
    stateMachine.transitionToFlushing();
  }

  public void cancel() {
    stateMachine.cancel();
  }

  public void abort() {
    stateMachine.abort();
  }

  public long getEndTime() {
    return executionEndTime.get();
  }

  public long getStartTime() {
    return executionStartTime.get();
  }

  public FragmentInstanceInfo getInstanceInfo() {
    return new FragmentInstanceInfo(
        stateMachine.getState(), getEndTime(), getFailedCause(), getFailureInfoList());
  }

  public FragmentInstanceStateMachine getStateMachine() {
    return stateMachine;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  public Optional<Throwable> getFailureCause() {
    return Optional.ofNullable(stateMachine.getFailureCauses().peek());
  }

  public ExecutorService getIntoOperationExecutor() {
    return intoOperationExecutor;
  }
}
