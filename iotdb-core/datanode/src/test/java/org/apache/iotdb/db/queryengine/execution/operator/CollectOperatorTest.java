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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.process.CollectOperator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CollectOperatorTest {

  @Test
  public void shouldConsumeReadyChildBeforeEarlierBlockedChild() throws Exception {
    SettableFuture<Void> firstChildBlocked = SettableFuture.create();
    TestOperator firstChild = new TestOperator(block(1), firstChildBlocked);
    TestOperator secondChild = new TestOperator(block(2), immediateVoidFuture());

    CollectOperator collectOperator =
        new CollectOperator(null, Arrays.asList(firstChild, secondChild));

    assertTrue(collectOperator.isBlocked().isDone());
    assertSame(secondChild.blocks.get(0), collectOperator.next());
    assertTrue(collectOperator.hasNext());
  }

  @Test
  public void shouldRotateReadyChildren() throws Exception {
    TestOperator firstChild =
        new TestOperator(Arrays.asList(block(1), block(3)), immediateVoidFuture());
    TestOperator secondChild =
        new TestOperator(Arrays.asList(block(2), block(4)), immediateVoidFuture());

    CollectOperator collectOperator =
        new CollectOperator(null, Arrays.asList(firstChild, secondChild));

    assertSame(firstChild.blocks.get(0), collectOperator.next());
    assertSame(secondChild.blocks.get(0), collectOperator.next());
    assertSame(firstChild.blocks.get(1), collectOperator.next());
    assertSame(secondChild.blocks.get(1), collectOperator.next());
  }

  private static TsBlock block(int value) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(value);
    builder.getColumnBuilder(0).writeInt(value);
    builder.declarePosition();
    return builder.build();
  }

  private static final class TestOperator implements Operator {
    private final List<TsBlock> blocks;
    private final ListenableFuture<?> blocked;
    private final OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    private int index;

    private TestOperator(TsBlock block, ListenableFuture<?> blocked) {
      this(Collections.singletonList(block), blocked);
    }

    private TestOperator(List<TsBlock> blocks, ListenableFuture<?> blocked) {
      this.blocks = blocks;
      this.blocked = blocked;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public TsBlock next() {
      return blocks.get(index++);
    }

    @Override
    public boolean hasNext() {
      return index < blocks.size();
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return blocked;
    }

    @Override
    public boolean isFinished() {
      return !hasNext();
    }

    @Override
    public void close() {}

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.shallowSizeOfInstance(TestOperator.class);
    }
  }
}
