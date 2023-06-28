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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.IMNode;

import java.util.NoSuchElementException;

public class TraverserWithLimitOffsetWrapper<R, N extends IMNode<N>> extends Traverser<R, N> {
  private final Traverser<R, N> traverser;
  private final long limit;
  private final long offset;
  private final boolean hasLimit;

  private int count = 0;
  int curOffset = 0;

  public TraverserWithLimitOffsetWrapper(Traverser<R, N> traverser, long limit, long offset) {
    this.traverser = traverser;
    this.limit = limit;
    this.offset = offset;
    hasLimit = limit > 0 || offset > 0;

    if (hasLimit) {
      while (curOffset < offset && traverser.hasNext()) {
        traverser.next();
        curOffset++;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (hasLimit) {
      return count < limit && traverser.hasNext();
    } else {
      return traverser.hasNext();
    }
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    R result = traverser.next();
    if (hasLimit) {
      count++;
    }
    return result;
  }

  @Override
  public void traverse() throws MetadataException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSuccess() {
    return traverser.isSuccess();
  }

  @Override
  public Throwable getFailure() {
    return traverser.getFailure();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(N node) {
    return false;
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(N node) {
    return false;
  }

  @Override
  protected boolean acceptInternalMatchedNode(N node) {
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(N node) {
    return false;
  }

  @Override
  protected R generateResult(N nextMatchedNode) {
    return null;
  }

  @Override
  protected boolean mayTargetNodeType(N node) {
    return false;
  }

  @Override
  public void close() {
    traverser.close();
  }

  @Override
  public void reset() {
    traverser.reset();
    count = 0;
    curOffset = 0;
    if (hasLimit) {
      while (curOffset < offset && traverser.hasNext()) {
        traverser.next();
        curOffset++;
      }
    }
  }

  public int getNextOffset() {
    return curOffset + count;
  }
}
