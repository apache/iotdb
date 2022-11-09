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

import org.apache.iotdb.db.mpp.execution.object.entry.SchemaFetchObjectEntry;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.object.ObjectProcessOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.List;

public class SchemaFetchMergeOperator extends ObjectProcessOperator<SchemaFetchObjectEntry>
    implements ProcessOperator {

  private final List<Operator> children;
  private final int childrenCount;

  private int currentIndex;

  private boolean isReadingStorageGroupInfo;

  private final List<String> storageGroupList;

  public SchemaFetchMergeOperator(
      OperatorContext operatorContext,
      String queryId,
      List<Operator> children,
      List<String> storageGroupList) {
    super(operatorContext, queryId);
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
  protected boolean hasNextBatch() {
    return isReadingStorageGroupInfo || currentIndex < childrenCount;
  }

  @Override
  protected List<SchemaFetchObjectEntry> nextBatch() {
    if (isReadingStorageGroupInfo) {
      isReadingStorageGroupInfo = false;
      return Collections.singletonList(new SchemaFetchObjectEntry(storageGroupList));
    }

    if (children.get(currentIndex).hasNext()) {
      return getNextObjectBatchFromChild(children.get(currentIndex));
    } else {
      currentIndex++;
      return null;
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return isReadingStorageGroupInfo || currentIndex >= children.size()
        ? NOT_BLOCKED
        : children.get(currentIndex).isBlocked();
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
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
