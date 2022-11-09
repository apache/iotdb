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

package org.apache.iotdb.db.mpp.execution.operator.object;

import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.object.MPPObjectPool;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import static org.apache.iotdb.db.mpp.execution.operator.object.ObjectQueryConstant.BATCH_CONTINUE_SYMBOL;
import static org.apache.iotdb.db.mpp.execution.operator.object.ObjectQueryConstant.BATCH_END_SYMBOL;
import static org.apache.iotdb.db.mpp.execution.operator.object.ObjectQueryConstant.NO_MORE_OBJECT_SYMBOL;
import static org.apache.iotdb.db.mpp.execution.operator.object.ObjectQueryConstant.OBJECT_START_SYMBOL;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class ObjectSerializeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final QueryId queryId;
  private final MPPObjectPool objectPool = MPPObjectPool.getInstance();

  private final List<TSDataType> outputDataTypes = Collections.singletonList(TSDataType.TEXT);

  private final Operator child;

  private final Queue<TsBlock> tsBlockBufferQueue = new LinkedList<>();

  public ObjectSerializeOperator(OperatorContext operatorContext, QueryId queryId, Operator child) {
    this.operatorContext = operatorContext;
    this.queryId = queryId;
    this.child = child;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (!tsBlockBufferQueue.isEmpty()) {
      return NOT_BLOCKED;
    } else {
      return child.isBlocked();
    }
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (tsBlockBufferQueue.isEmpty()) {
      TsBlock tsBlock = child.next();
      SegmentedByteOutputStream segmentedByteOutputStream =
          new SegmentedByteOutputStream((int) (DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES * 0.8));
      DataOutputStream dataOutputStream = new DataOutputStream(segmentedByteOutputStream);
      try {
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          dataOutputStream.write(OBJECT_START_SYMBOL);
          objectPool.get(queryId, tsBlock.getColumn(0).getInt(i)).serializeObject(dataOutputStream);
        }
        dataOutputStream.write(NO_MORE_OBJECT_SYMBOL);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      List<ByteBuffer> bufferList = segmentedByteOutputStream.getBufferList();

      for (int i = 0; i < bufferList.size(); i++) {
        TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeBinary(new Binary(bufferList.get(0).array()));
        if (i == bufferList.size() - 1) {
          builder.getColumnBuilder(0).writeBinary(new Binary(BATCH_END_SYMBOL));
        } else {
          builder.getColumnBuilder(0).writeBinary(new Binary(BATCH_CONTINUE_SYMBOL));
        }
        tsBlockBufferQueue.offer(builder.build());
      }
    }

    return tsBlockBufferQueue.poll();
  }

  @Override
  public boolean hasNext() {
    return !tsBlockBufferQueue.isEmpty() || child.hasNext();
  }

  @Override
  public boolean isFinished() {
    return tsBlockBufferQueue.isEmpty() && child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory()
        + child.calculateMaxReturnSize()
        - calculateMaxReturnSize();
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateMaxReturnSize()
        - calculateMaxReturnSize()
        + child.calculateRetainedSizeAfterCallingNext();
  }

  private static class SegmentedByteOutputStream extends OutputStream {

    private final List<ByteBuffer> bufferList = new ArrayList<>();

    private final int bufferSize;

    private ByteBuffer workingBuffer;

    public SegmentedByteOutputStream(int bufferSize) {
      this.bufferSize = bufferSize;
      workingBuffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void write(int b) throws IOException {
      if (!workingBuffer.hasRemaining()) {
        getNewBuffer();
      }
      workingBuffer.put((byte) b);
    }

    public List<ByteBuffer> getBufferList() {
      getNewBuffer();
      return bufferList;
    }

    private void getNewBuffer() {
      workingBuffer.flip();
      bufferList.add(workingBuffer);
      workingBuffer = ByteBuffer.allocate(bufferSize);
    }
  }
}
