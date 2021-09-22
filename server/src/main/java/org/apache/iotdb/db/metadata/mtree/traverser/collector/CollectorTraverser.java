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
package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

// This class defines the generic resultSet as traversal result and add more restrictions on MTree
// traversal.
public abstract class CollectorTraverser<T> extends Traverser {

  // used for implement slimit and offset function in DDL
  protected int limit;
  protected int offset;

  protected boolean hasLimit = false;
  protected int count = 0;
  protected int curOffset = -1;

  protected T resultSet;

  public CollectorTraverser(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
  }

  public CollectorTraverser(IMNode startNode, PartialPath path, int limit, int offset)
      throws MetadataException {
    super(startNode, path);
    this.limit = limit;
    this.offset = offset;
    if (limit != 0 || offset != 0) {
      hasLimit = true;
    }
  }

  /** extends traversal with limit restriction */
  @Override
  protected void traverse(IMNode node, int idx, int level) throws MetadataException {
    if (hasLimit && count == limit) {
      return;
    }
    super.traverse(node, idx, level);
  }

  /**
   * After invoke traverse(), this method could be invoked to get result
   *
   * @return the traversal result
   */
  public T getResult() {
    return resultSet;
  }

  public void setResultSet(T resultSet) {
    this.resultSet = resultSet;
  }

  public int getCurOffset() {
    return curOffset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
    if (limit != 0) {
      hasLimit = true;
    }
  }

  public void setOffset(int offset) {
    this.offset = offset;
    if (offset != 0) {
      hasLimit = true;
    }
  }
}
