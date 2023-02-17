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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class SchemaFetchMergeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Operator> children;
  private final int childrenCount;

  private int currentIndex;

  private boolean isReadingStorageGroupInfo;

  private final List<String> storageGroupList;

  public SchemaFetchMergeOperator(
      OperatorContext operatorContext, List<Operator> children, List<String> storageGroupList) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.childrenCount = children.size();

    this.currentIndex = 0;

    this.isReadingStorageGroupInfo = true;

    this.storageGroupList = storageGroupList;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    if (isReadingStorageGroupInfo) {
      isReadingStorageGroupInfo = false;
      return generateStorageGroupInfo();
    }

    if (children.get(currentIndex).hasNextWithTimer()) {
      return children.get(currentIndex).nextWithTimer();
    } else {
      currentIndex++;
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return isReadingStorageGroupInfo || currentIndex < childrenCount;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return isReadingStorageGroupInfo || currentIndex >= children.size()
        ? NOT_BLOCKED
        : children.get(currentIndex).isBlocked();
  }

  @Override
  public boolean isFinished() {
    return !hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  private TsBlock generateStorageGroupInfo() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      // to indicate this binary data is database info
      ReadWriteIOUtils.write((byte) 0, outputStream);

      ReadWriteIOUtils.write(storageGroupList.size(), outputStream);
      for (String storageGroup : storageGroupList) {
        ReadWriteIOUtils.write(storageGroup, outputStream);
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
    return new TsBlock(
        new TimeColumn(1, new long[] {0}),
        new BinaryColumn(
            1, Optional.empty(), new Binary[] {new Binary(outputStream.toByteArray())}));
  }

  @Override
  public long calculateMaxPeekMemory() {
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
    }

    return childrenMaxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long childrenMaxReturnSize = 0;
    for (Operator child : children) {
      childrenMaxReturnSize = Math.max(childrenMaxReturnSize, child.calculateMaxReturnSize());
    }

    return childrenMaxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long retainedSize = 0L;
    for (Operator child : children) {
      retainedSize += child.calculateRetainedSizeAfterCallingNext();
    }
    return retainedSize;
  }
}
