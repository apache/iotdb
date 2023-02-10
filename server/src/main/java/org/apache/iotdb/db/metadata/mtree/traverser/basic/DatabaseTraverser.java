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
package org.apache.iotdb.db.metadata.mtree.traverser.basic;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

public abstract class DatabaseTraverser<R> extends Traverser<R> {

  protected boolean collectInternal = false;

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @throws MetadataException path does not meet the expected rules
   */
  public DatabaseTraverser(
      IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch);
  }

  @Override
  protected boolean acceptFullMatchedNode(IMNode node) {
    return node.isStorageGroup();
  }

  @Override
  protected boolean acceptInternalMatchedNode(IMNode node) {
    return collectInternal && node.isStorageGroup();
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(IMNode node) {
    return !node.isStorageGroup();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(IMNode node) {
    return !node.isStorageGroup();
  }

  public void setCollectInternal(boolean collectInternal) {
    this.collectInternal = collectInternal;
  }
}
