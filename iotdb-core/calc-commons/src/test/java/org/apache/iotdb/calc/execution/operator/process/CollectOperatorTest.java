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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CollectOperatorTest {
  private static TsBlock block(int... values) {
    TsBlockBuilder builder =
        new TsBlockBuilder(Collections.nCopies(values.length, TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(0);
    for (int i = 0; i < values.length; i++) {
      builder.getColumnBuilder(i).writeInt(values[i]);
    }
    builder.declarePosition();
    return builder.build();
  }

  private static CollectOperator collect(Operator... children) {
    CollectOperator collect =
        new CollectOperator(new TestContext(), new ArrayList<>(Arrays.asList(children)));
    return collect;
  }

  @Test
  public void testReadyChildAndStableSelectionDuringBackpressure() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    b.ready();
    CollectOperator collect = collect(a, b);
    ListenableFuture<?> selected = collect.isBlocked();
    assertTrue(selected.isDone());
    a.ready();
    assertSame(selected, collect.isBlocked());
    assertSame(b.output, collect.next());
    assertEquals(0, a.advances);
    assertEquals(1, b.advances);
    assertTrue(collect.isBlocked().isDone());
    assertSame(a.output, collect.next());
    collect.close();
  }

  @Test
  public void testThousandChildrenAndOneListenerPerPendingPeriod() throws Exception {
    for (int count : new int[] {100, 1000}) {
      List<Operator> children = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        children.add(new Child(block(i)));
      }
      CollectOperator collect = new CollectOperator(new TestContext(), children);
      Child ready = (Child) children.get(count - 1);
      for (int round = 0; round < 5; round++) {
        ListenableFuture<?> wait = collect.isBlocked();
        assertFalse(wait.isDone());
        for (int repeat = 0; repeat < 10; repeat++) {
          assertSame(wait, collect.isBlocked());
        }
        ready.ready();
        wait.get(5, TimeUnit.SECONDS);
        assertSame(ready.output, collect.next());
        ready.reset();
      }
      for (int i = 0; i < count - 1; i++) {
        Child child = (Child) children.get(i);
        assertEquals(1, child.waitCalls);
        assertEquals(1, child.wait.registrations.get());
      }
      collect.close();
    }
  }

  @Test
  public void testAllReadyChildrenHaveLinearSelectionCost() throws Exception {
    int childCount = 1000;
    int rounds = 5;
    List<Child> children = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      Child child = new Child(block(i));
      child.ready();
      children.add(child);
    }
    CountingList countedChildren = new CountingList(children);
    CollectOperator collect = new CollectOperator(new TestContext(), countedChildren);
    try {
      for (int round = 0; round < rounds; round++) {
        assertTrue(collect.isBlocked().isDone());
        // A completed future may notify more than once. It must occupy only one ready slot.
        for (Child child : children) {
          child.wait.listener.run();
          child.wait.listener.run();
        }
        Set<Integer> consumed = new HashSet<>();
        for (int i = 0; i < childCount; i++) {
          assertTrue(collect.isBlocked().isDone());
          TsBlock result = collect.next();
          assertTrue(
              "every ready child must be selected once per round",
              consumed.add(result.getColumn(0).getInt(0)));
        }
        assertEquals(childCount, consumed.size());
      }
      for (Child child : children) {
        assertEquals(rounds, child.advances);
      }
      long probes = countedChildren.getCalls + countFutureProbes(children);
      long linearLimit = 20L * (childCount + childCount * rounds);
      assertTrue(
          "selection must use O(children + blocks) probes, observed " + probes,
          probes <= linearLimit);
    } finally {
      collect.close();
    }
  }

  @Test
  public void testClosedChildrenDoNotIncreaseRemainingChildSelectionCost() throws Exception {
    int childCount = 1000;
    int remainingBlocks = 3000;
    List<Child> children = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      Child child = new Child(block(i));
      child.hasNext = i == childCount - 1;
      child.finished = !child.hasNext;
      child.ready();
      children.add(child);
    }
    CountingList countedChildren = new CountingList(children);
    CollectOperator collect = new CollectOperator(new TestContext(), countedChildren);
    try {
      // Retire the first 999 children and consume one block from the remaining child.
      for (int i = 0; i < childCount; i++) {
        assertTrue(collect.isBlocked().isDone());
        collect.next();
      }
      for (int i = 0; i < childCount - 1; i++) {
        assertEquals(1, children.get(i).closes);
      }
      Child remaining = children.get(childCount - 1);
      assertEquals(1, remaining.advances);
      long initialProbes = countedChildren.getCalls + countFutureProbes(children);
      for (int i = 0; i < remainingBlocks; i++) {
        assertTrue(collect.isBlocked().isDone());
        assertSame(remaining.output, collect.next());
      }
      assertEquals(remainingBlocks + 1, remaining.advances);
      long probes = countedChildren.getCalls + countFutureProbes(children) - initialProbes;
      assertTrue(
          "retired children must not be visited for each remaining block, observed " + probes,
          probes <= 20L * remainingBlocks);
    } finally {
      collect.close();
    }
  }

  private static long countFutureProbes(List<Child> children) {
    long probes = 0;
    for (Child child : children) {
      probes += child.wait.probes.get();
    }
    return probes;
  }

  @Test
  public void testPendingListenerReusedAndStaleCompletionIgnored() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    CollectOperator collect = collect(a, b);
    ListenableFuture<?> first = collect.isBlocked();
    b.ready();
    assertSame(b.output, collect.next());
    Runnable stale = b.wait.listener;
    b.reset();
    ListenableFuture<?> second = collect.isBlocked();
    assertFalse(second.isDone());
    stale.run();
    assertFalse(second.isDone());
    a.ready();
    second.get(5, TimeUnit.SECONDS);
    assertEquals(1, a.wait.registrations.get());
    assertSame(a.output, collect.next());
    assertTrue(first.isDone());
    collect.close();
  }

  @Test
  public void testCompletionBetweenSelectionsIsNotLost() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    b.ready();
    CollectOperator collect = collect(a, b);
    try {
      assertTrue(collect.isBlocked().isDone());
      assertSame(b.output, collect.next());
      b.reset();
      // No selection is waiting yet: retain this completion until the next driver observation.
      a.ready();
      assertTrue(collect.isBlocked().isDone());
      assertSame(a.output, collect.next());
      a.reset();
      assertFalse(collect.isBlocked().isDone());
      assertEquals(1, a.advances);
      assertEquals(1, b.advances);
    } finally {
      collect.close();
    }
  }

  @Test
  public void testNullEmptyAndUnfinishedChildRefreshReadiness() throws Exception {
    Child child = new Child(null);
    child.ready();
    CollectOperator collect = collect(child);
    assertTrue(collect.isBlocked().isDone());
    assertNull(collect.next());
    child.reset();
    assertFalse(collect.isBlocked().isDone());
    child.output = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32)).build();
    child.ready();
    assertTrue(collect.next().isEmpty());
    child.reset();
    child.hasNext = false;
    child.ready();
    assertNull(collect.next());
    assertFalse(collect.isFinished());
    assertEquals(0, child.closes);
    child.finished = true;
    assertNull(collect.next());
    assertTrue(collect.isFinished());
    assertEquals(1, child.closes);
    assertTrue(collect.isBlocked().isDone());
  }

  @Test
  public void testCancellationDoesNotPropagateToChild() throws Exception {
    Child child = new Child(block(1));
    CollectOperator collect = collect(child);
    ListenableFuture<?> exposed = collect.isBlocked();
    assertTrue(exposed.cancel(true));
    assertFalse(child.wait.isCancelled());
    ListenableFuture<?> replacement = collect.isBlocked();
    assertFalse(replacement.isCancelled());
    child.ready();
    replacement.get(5, TimeUnit.SECONDS);
    assertSame(child.output, collect.next());
    collect.close();
  }

  @Test
  public void testFailureOfUnselectedChildIsNotLost() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    a.ready();
    CollectOperator collect = collect(a, b);
    assertTrue(collect.isBlocked().isDone());
    IOException expected = new IOException("child failure");
    b.wait.delegate.setException(expected);
    try {
      collect.next();
      fail("must propagate the failed child even when another child was selected");
    } catch (IOException e) {
      assertSame(expected, e);
    } finally {
      collect.close();
    }
    assertEquals(0, a.advances);
  }

  @Test
  public void testChildCancellationPropagates() throws Exception {
    Child child = new Child(block(1));
    CollectOperator collect = collect(child);
    ListenableFuture<?> wait = collect.isBlocked();
    child.wait.cancel(false);
    assertTrue(wait.isDone());
    try {
      collect.next();
      fail("must propagate child cancellation");
    } catch (CancellationException expected) {
      assertEquals(0, child.advances);
    } finally {
      collect.close();
    }
  }

  @Test
  public void testCompletionDuringRegistrationAndOnAnotherThread() throws Exception {
    Child a = new Child(block(1));
    a.wait.beforeRegistration = a::ready;
    CollectOperator collect = collect(a);
    assertTrue(collect.isBlocked().isDone());
    assertSame(a.output, collect.next());
    a.reset();
    ListenableFuture<?> pending = collect.isBlocked();
    Thread completion = new Thread(a::ready);
    completion.start();
    pending.get(5, TimeUnit.SECONDS);
    completion.join();
    assertSame(a.output, collect.next());
    collect.close();
  }

  @Test
  public void testCloseWakesWaitAndClosesEveryChildDespiteException() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    a.closeFailure = new IOException("close failure");
    b.closeFailure = new IOException("second close failure");
    CollectOperator collect = collect(a, b);
    ListenableFuture<?> pending = collect.isBlocked();
    try {
      collect.close();
      fail("must propagate close failure");
    } catch (IOException e) {
      assertSame(a.closeFailure, e);
      assertEquals(1, e.getSuppressed().length);
      assertSame(b.closeFailure, e.getSuppressed()[0]);
    }
    pending.get(5, TimeUnit.SECONDS);
    assertEquals(1, a.closes);
    assertEquals(1, b.closes);
    a.ready();
    b.ready();
    assertTrue(collect.isFinished());
    assertNull(collect.next());
    collect.close();
    assertEquals(1, b.closes);
  }

  @Test
  public void testMappingUsesActualChildIdentity() throws Exception {
    Child a = new Child(block(10, 11));
    Child b = new Child(block(20, 21));
    b.ready();
    List<List<Integer>> mappings = new ArrayList<>();
    mappings.add(Collections.singletonList(0));
    mappings.add(Collections.singletonList(1));
    MappingCollectOperator collect =
        new MappingCollectOperator(
            new TestContext(), new ArrayList<>(Arrays.asList(a, b)), mappings);
    assertTrue(collect.isBlocked().isDone());
    assertEquals(21, collect.next().getColumn(0).getInt(0));
    a.ready();
    assertEquals(10, collect.next().getColumn(0).getInt(0));
    collect.close();
    assertNull(mappings.get(0));
    assertNull(mappings.get(1));
  }

  @Test
  public void testMemoryIncludesAllLiveRetainedStates() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    a.retained = 30;
    a.peak = 100;
    a.returnSize = 10;
    b.retained = 50;
    b.peak = 80;
    b.returnSize = 20;
    CollectOperator collect = collect(a, b);
    assertEquals(80, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(150, collect.calculateMaxPeekMemory());
    assertEquals(20, collect.calculateMaxReturnSize());
    assertTrue(collect.ramBytesUsed() > 0);
    a.hasNext = false;
    a.finished = true;
    a.ready();
    collect.next();
    assertEquals(50, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(80, collect.calculateMaxPeekMemory());
    collect.close();
    assertEquals(0, collect.calculateMaxPeekMemory());
    assertEquals(0, collect.calculateMaxReturnSize());
    assertEquals(0, collect.calculateRetainedSizeAfterCallingNext());
  }

  @Test
  public void testEmptyCollectIsAlreadyFinished() throws Exception {
    CollectOperator collect = collect();
    assertFalse(collect.hasNext());
    assertTrue(collect.isFinished());
    assertTrue(collect.isBlocked().isDone());
    assertNull(collect.next());
    assertEquals(0, collect.calculateMaxPeekMemory());
    assertEquals(0, collect.calculateMaxReturnSize());
    collect.close();
    collect.close();
  }

  @Test
  public void testFailureWakesAllPendingChildren() throws Exception {
    Child a = new Child(block(1));
    Child b = new Child(block(2));
    CollectOperator collect = collect(a, b);
    ListenableFuture<?> pending = collect.isBlocked();
    assertFalse(pending.isDone());
    IOException expected = new IOException("blocked child failure");
    b.wait.delegate.setException(expected);
    try {
      pending.get(5, TimeUnit.SECONDS);
      fail("child failure must wake the driver");
    } catch (ExecutionException e) {
      assertSame(expected, e.getCause());
    }
    try {
      collect.next();
      fail("child failure must propagate without advancing other children");
    } catch (IOException e) {
      assertSame(expected, e);
    } finally {
      collect.close();
    }
    assertEquals(0, a.advances);
    assertEquals(0, b.advances);
    assertEquals(1, a.closes);
    assertEquals(1, b.closes);
  }

  @Test
  public void testConcurrentCompletionsConsumeEveryChildExactlyOnce() throws Exception {
    int count = 16;
    List<Child> children = new ArrayList<>();
    for (int i = 0; i < count; i++) children.add(new Child(block(i)));
    CollectOperator collect = new CollectOperator(new TestContext(), new ArrayList<>(children));
    ListenableFuture<?> pending = collect.isBlocked();
    assertFalse(pending.isDone());
    ExecutorService executor = Executors.newFixedThreadPool(count);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> completions = new ArrayList<>();
    try {
      for (Child child : children) {
        completions.add(
            executor.submit(
                () -> {
                  start.await();
                  child.ready();
                  return null;
                }));
      }
      start.countDown();
      pending.get(5, TimeUnit.SECONDS);
      for (Future<?> completion : completions) completion.get(5, TimeUnit.SECONDS);
      Set<Integer> consumed = new HashSet<>();
      for (int round = 0; round < 2 * count && collect.hasNext(); round++) {
        assertTrue(collect.isBlocked().isDone());
        TsBlock result = collect.next();
        if (result != null) {
          int childId = result.getColumn(0).getInt(0);
          assertTrue("child emitted more than once", consumed.add(childId));
          children.get(childId).hasNext = false;
          children.get(childId).finished = true;
        }
      }
      assertEquals(count, consumed.size());
      assertTrue(collect.isFinished());
      assertNull(collect.next());
      for (Child child : children) {
        assertEquals(1, child.advances);
        assertEquals(1, child.closes);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      collect.close();
    }
  }

  private static final class CountingList extends AbstractList<Operator> {
    private final List<Operator> delegate;
    private long getCalls;

    private CountingList(List<? extends Operator> children) {
      delegate = new ArrayList<>(children);
    }

    @Override
    public Operator get(int index) {
      getCalls++;
      return delegate.get(index);
    }

    @Override
    public Operator set(int index, Operator child) {
      return delegate.set(index, child);
    }

    @Override
    public int size() {
      return delegate.size();
    }
  }

  private static final class CountingFuture
      extends ForwardingListenableFuture.SimpleForwardingListenableFuture<Void> {
    private final SettableFuture<Void> delegate;
    private final AtomicInteger registrations = new AtomicInteger();
    private final AtomicInteger probes = new AtomicInteger();
    private Runnable beforeRegistration;
    private Runnable listener;

    private CountingFuture() {
      this(SettableFuture.create());
    }

    private CountingFuture(SettableFuture<Void> delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public boolean isDone() {
      probes.incrementAndGet();
      return super.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      probes.incrementAndGet();
      return super.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      probes.incrementAndGet();
      return super.get(timeout, unit);
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      this.listener = listener;
      registrations.incrementAndGet();
      if (beforeRegistration != null) {
        beforeRegistration.run();
      }
      super.addListener(listener, executor);
    }
  }

  private static final class TestContext extends CommonOperatorContext {
    private TestContext() {
      super(0, new PlanNodeId("collect"), "test");
    }

    @Override
    public MemoryReservationManager getMemoryReservationContext() {
      return null;
    }

    @Override
    public int getFragmentId() {
      return 0;
    }

    @Override
    public int getPipelineId() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  private static final class Child implements Operator {
    private final Thread driver = Thread.currentThread();
    private final CommonOperatorContext context = new TestContext();
    private CountingFuture wait = new CountingFuture();
    private TsBlock output;
    private boolean hasNext = true;
    private boolean finished;
    private int advances;
    private int waitCalls;
    private int closes;
    private IOException closeFailure;
    private long retained, peak, returnSize;

    private Child(TsBlock output) {
      this.output = output;
    }

    private void ready() {
      wait.delegate.set(null);
    }

    private void reset() {
      wait = new CountingFuture();
    }

    private void checkDriver() {
      assertSame(driver, Thread.currentThread());
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      checkDriver();
      waitCalls++;
      return wait;
    }

    @Override
    public boolean hasNext() {
      checkDriver();
      return hasNext;
    }

    @Override
    public TsBlock next() {
      checkDriver();
      advances++;
      return output;
    }

    @Override
    public boolean isFinished() {
      checkDriver();
      return finished;
    }

    @Override
    public void close() throws Exception {
      checkDriver();
      closes++;
      if (closeFailure != null) throw closeFailure;
    }

    @Override
    public long calculateMaxPeekMemory() {
      return peak;
    }

    @Override
    public long calculateMaxReturnSize() {
      return returnSize;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return retained;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }
}
