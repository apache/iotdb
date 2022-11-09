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

import org.apache.iotdb.db.mpp.execution.object.MPPObjectPool;
import org.apache.iotdb.db.mpp.execution.object.ObjectEntry;
import org.apache.iotdb.db.mpp.execution.object.ObjectEntryFactory;
import org.apache.iotdb.db.mpp.execution.object.ObjectType;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.iotdb.db.mpp.execution.operator.object.ObjectQueryConstant.BATCH_END_SYMBOL;
import static org.apache.iotdb.db.mpp.execution.operator.object.ObjectQueryConstant.OBJECT_START_SYMBOL;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class ObjectDeserializeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final String queryId;
  private final MPPObjectPool objectPool = MPPObjectPool.getInstance();

  private final List<TSDataType> outputDataTypes = Collections.singletonList(TSDataType.INT32);

  private final Operator child;

  private final List<ByteBuffer> bufferList = new ArrayList<>();

  public ObjectDeserializeOperator(
      OperatorContext operatorContext, String queryId, Operator child) {
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
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    TsBlock tsBlock = child.next();
    if (tsBlock == null || tsBlock.isEmpty()) {
      return null;
    }
    ByteBuffer buffer;
    for (int i = 0; i < tsBlock.getPositionCount() - 1; i++) {
      buffer = ByteBuffer.wrap(tsBlock.getColumn(0).getBinary(i).getValues());
      buffer.flip();
      bufferList.add(buffer);
    }
    if (Arrays.equals(
        tsBlock.getColumn(0).getBinary(tsBlock.getPositionCount() - 1).getValues(),
        BATCH_END_SYMBOL)) {
      return generateObject();
    } else {
      return null;
    }
  }

  private TsBlock generateObject() {
    SegmentedInputStream segmentedInputStream = new SegmentedInputStream(bufferList);
    DataInputStream dataInputStream = new DataInputStream(segmentedInputStream);
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    try {
      byte objectRecordSymbol = dataInputStream.readByte();
      while (objectRecordSymbol == OBJECT_START_SYMBOL) {
        ObjectEntry objectEntry =
            ObjectEntryFactory.getObjectEntry(ObjectType.deserialize(segmentedInputStream));
        objectEntry.deserializeObject(dataInputStream);
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeInt(objectPool.put(queryId, objectEntry).getId());
        objectRecordSymbol = dataInputStream.readByte();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    bufferList.clear();
    return builder.build();
  }

  @Override
  public boolean hasNext() {
    return !bufferList.isEmpty() || child.hasNext();
  }

  @Override
  public boolean isFinished() {
    return bufferList.isEmpty() && child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return (long) (bufferList.size()) * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES
        + child.calculateRetainedSizeAfterCallingNext();
  }

  private static class SegmentedInputStream extends InputStream {

    private final List<ByteBuffer> bufferList;

    private int index = 0;

    public SegmentedInputStream(List<ByteBuffer> bufferList) {
      this.bufferList = bufferList;
    }

    @Override
    public int read() throws IOException {
      ByteBuffer buffer = bufferList.get(index);
      if (!buffer.hasRemaining()) {
        if (index == bufferList.size() - 1) {
          throw new EOFException();
        } else {
          index++;
          buffer = bufferList.get(index);
        }
      }
      return buffer.get();
    }
  }
}
