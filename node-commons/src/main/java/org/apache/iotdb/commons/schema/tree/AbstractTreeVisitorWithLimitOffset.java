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

package org.apache.iotdb.commons.schema.tree;

import org.apache.iotdb.commons.path.PartialPath;

/**
 * This class defines a dfs-based traversing algorithm with limit and offset based on
 * AbstractTreeVisitor.
 *
 * <p>This class takes two extra parameters as input:
 *
 * <ol>
 *   <li>int limit: the max count of the results collected by one traversing process.
 *   <li>int offset: the index of first matched node to be collected.
 * </ol>
 */
public abstract class AbstractTreeVisitorWithLimitOffset<N extends ITreeNode, R>
    extends AbstractTreeVisitor<N, R> {

  protected final int limit;
  protected final int offset;
  protected final boolean hasLimit;

  protected int count = 0;
  protected int curOffset = -1;

  protected AbstractTreeVisitorWithLimitOffset(
      N root, PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch) {
    super(root, pathPattern, isPrefixMatch);
    this.limit = limit;
    this.offset = offset;
    hasLimit = limit != 0;
  }

  @Override
  public boolean hasNext() {
    if (hasLimit) {
      return count < limit && super.hasNext();
    }

    return super.hasNext();
  }

  @Override
  protected void getNext() {
    if (hasLimit) {
      if (curOffset < offset) {
        while (curOffset < offset) {
          super.getNext();
          curOffset += 1;
          if (nextMatchedNode == null) {
            return;
          }
        }
      } else {
        super.getNext();
        curOffset += 1;
      }
    } else {
      super.getNext();
    }
  }

  @Override
  public R next() {
    R result = super.next();
    if (hasLimit) {
      count++;
    }
    return result;
  }

  @Override
  public void reset() {
    super.reset();
    count = 0;
    curOffset = -1;
  }

  public int getNextOffset() {
    return curOffset + 1;
  }
}
