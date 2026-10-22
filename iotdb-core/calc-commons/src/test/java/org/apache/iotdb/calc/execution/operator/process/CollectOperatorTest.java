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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CollectOperatorTest {
  @Test
  public void testConsumesReadyChildAndFinishesOutOfOrder() throws Exception {
    TestingOperator first = new TestingOperator();
    TestingOperator second = new TestingOperator();
    CollectOperator collector = collect(first, second);
    ListenableFuture<?> waiting = collector.isBlocked();
    assertFalse(waiting.isDone());

    TsBlock secondBlock = block(20);
    second.addBlock(secondBlock);
    assertTrue(waiting.isDone());
    assertSame(secondBlock, collector.next());
    assertEquals(1, collector.currentIndex);

    second.finish();
    assertNull(collector.next());
    assertNull(collector.getChildren().get(1));
    assertEquals(1, second.closeCalls);
    assertTrue(collector.hasNext());
    assertFalse(collector.isFinished());
    assertFalse(collector.isBlocked().isDone());

    TsBlock firstBlock = block(10);
    first.addBlock(firstBlock);
    assertSame(firstBlock, collector.next());
    first.finish();
    assertNull(collector.next());
    assertFalse(collector.hasNext());
    assertTrue(collector.isFinished());
    assertTrue(collector.isBlocked().isDone());
    assertNull(collector.next());
    collector.close();
    assertEquals(1, first.closeCalls);
    assertEquals(1, second.closeCalls);
  }

  @Test
  public void testCloseIncludesEarlierChildrenAndReleasesWaiting() throws Exception {
    TestingOperator first = new TestingOperator();
    TestingOperator second = new TestingOperator();
    CollectOperator collector = collect(first, second);
    TsBlock result = block(20);
    second.addBlock(result);
    assertSame(result, collector.next());
    ListenableFuture<?> waiting = collector.isBlocked();
    assertFalse(waiting.isDone());

    collector.close();
    assertTrue(waiting.isDone());
    assertTrue(collector.isFinished());
    assertFalse(collector.hasNext());
    first.finish();
    second.finish();
    assertNull(collector.next());
    collector.close();
    assertEquals(1, first.closeCalls);
    assertEquals(1, second.closeCalls);
  }

  @Test
  public void testYieldRefreshesOnlyConsumedChild() throws Exception {
    TestingOperator first = new TestingOperator();
    TestingOperator second = new TestingOperator();
    CollectOperator collector = collect(first, second);
    collector.isBlocked();
    second.blocked.set(null);
    assertNull(collector.next());
    ListenableFuture<?> waiting = collector.isBlocked();
    assertFalse(waiting.isDone());
    assertEquals(1, first.blockedCalls);
    assertEquals(2, second.blockedCalls);
    assertSame(waiting, collector.isBlocked());

    TsBlock result = block(20);
    second.addBlock(result);
    assertSame(result, collector.next());
    assertEquals(0, first.nextCalls);
    assertEquals(2, second.nextCalls);
    collector.close();
  }

  @Test
  public void testMappingUsesReadyChildAndClosesOutOfOrder() throws Exception {
    TestingOperator first = new TestingOperator();
    TestingOperator second = new TestingOperator();
    List<List<Integer>> mappings =
        new ArrayList<>(Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 0)));
    MappingCollectOperator collector =
        new MappingCollectOperator(null, children(first, second), mappings);
    ListenableFuture<?> waiting = collector.isBlocked();
    second.addBlock(block(20, 21));
    assertTrue(waiting.isDone());
    TsBlock result = collector.next();
    assertEquals(21, result.getColumn(0).getInt(0));
    assertEquals(20, result.getColumn(1).getInt(0));

    second.finish();
    assertNull(collector.next());
    assertNull(mappings.get(1));
    assertTrue(collector.hasNext());
    first.addBlock(block(10, 11));
    result = collector.next();
    assertEquals(10, result.getColumn(0).getInt(0));
    assertEquals(11, result.getColumn(1).getInt(0));
    first.finish();
    assertNull(collector.next());
    assertNull(mappings.get(0));
    assertTrue(collector.isFinished());
    collector.close();
    assertEquals(1, first.closeCalls);
    assertEquals(1, second.closeCalls);
  }

  @Test
  public void testAnotherCompletionCannotChangeSelectedMapping() throws Exception {
    TestingOperator first = new TestingOperator();
    TestingOperator second = new TestingOperator();
    MappingCollectOperator collector =
        new MappingCollectOperator(
            null,
            children(first, second),
            new ArrayList<>(Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 0))));
    collector.isBlocked();
    second.addBlock(block(20, 21));
    second.onNext = () -> first.addBlock(block(10, 11));
    TsBlock result = collector.next();
    assertEquals(21, result.getColumn(0).getInt(0));
    assertEquals(20, result.getColumn(1).getInt(0));
    assertEquals(1, collector.currentIndex);
    result = collector.next();
    assertEquals(10, result.getColumn(0).getInt(0));
    assertEquals(11, result.getColumn(1).getInt(0));
    collector.close();
  }

  @Test
  public void testMemoryIncludesOtherRetainedChildren() throws Exception {
    TestingOperator first = new TestingOperator();
    first.peekMemory = 100;
    first.retainedMemory = 30;
    first.returnSize = 40;
    TestingOperator second = new TestingOperator();
    second.peekMemory = 200;
    second.retainedMemory = 50;
    second.returnSize = 60;
    CollectOperator collector = collect(first, second);
    assertEquals(80, collector.calculateRetainedSizeAfterCallingNext());
    assertEquals(230, collector.calculateMaxPeekMemory());
    assertEquals(60, collector.calculateMaxReturnSize());
    assertTrue(collector.ramBytesUsed() > 0);

    first.finish();
    assertNull(collector.next());
    assertEquals(50, collector.calculateRetainedSizeAfterCallingNext());
    assertEquals(200, collector.calculateMaxPeekMemory());
    assertEquals(60, collector.calculateMaxReturnSize());
    collector.close();
    assertEquals(0, collector.calculateRetainedSizeAfterCallingNext());
    assertEquals(0, collector.calculateMaxPeekMemory());
    assertEquals(0, collector.calculateMaxReturnSize());
  }

  @Test
  public void testEmptyChildren() throws Exception {
    CollectOperator collector = collect();
    assertTrue(collector.isFinished());
    assertFalse(collector.hasNext());
    assertTrue(collector.isBlocked().isDone());
    assertNull(collector.next());
    collector.close();
  }

  private static CollectOperator collect(TestingOperator... children) {
    return new CollectOperator(null, children(children));
  }

  private static List<Operator> children(TestingOperator... children) {
    return new ArrayList<>(Arrays.asList(children));
  }

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

  private static final class TestingOperator implements Operator {
    private SettableFuture<Void> blocked = SettableFuture.create();
    private final Deque<TsBlock> blocks = new ArrayDeque<>();
    private boolean finished;
    private int blockedCalls;
    private int nextCalls;
    private int closeCalls;
    private long peekMemory;
    private long retainedMemory;
    private long returnSize;
    private Runnable onNext;

    private void addBlock(TsBlock block) {
      blocks.addLast(block);
      blocked.set(null);
    }

    private void finish() {
      finished = true;
      blocked.set(null);
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return null;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      blockedCalls++;
      return blocked;
    }

    @Override
    public boolean hasNext() {
      assertTrue("Do not probe blocked children", blocked.isDone());
      return !finished || !blocks.isEmpty();
    }

    @Override
    public boolean hasNextWithTimer() {
      return hasNext();
    }

    @Override
    public TsBlock next() {
      assertTrue("Do not consume blocked children", blocked.isDone());
      nextCalls++;
      TsBlock block = blocks.pollFirst();
      blocked = SettableFuture.create();
      if (finished || !blocks.isEmpty()) {
        blocked.set(null);
      }
      if (onNext != null) {
        onNext.run();
      }
      return block;
    }

    @Override
    public TsBlock nextWithTimer() {
      return next();
    }

    @Override
    public void close() {
      closeCalls++;
    }

    @Override
    public boolean isFinished() {
      return finished && blocks.isEmpty();
    }

    @Override
    public long calculateMaxPeekMemory() {
      return peekMemory;
    }

    @Override
    public long calculateMaxPeekMemoryWithCounter() {
      return peekMemory;
    }

    @Override
    public long calculateMaxReturnSize() {
      return returnSize;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return retainedMemory;
    }

    @Override
    public long ramBytesUsed() {
      return 64;
    }
  }
}
