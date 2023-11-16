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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.ICacheManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class PBTreeFlushExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PBTreeFlushExecutor.class);

  private final ICachedMNode subtreeRoot;

  private final ICacheManager cacheManager;

  private final ISchemaFile file;

  private final LockManager lockManager;

  public PBTreeFlushExecutor(
      ICachedMNode subtreeRoot,
      ICacheManager cacheManager,
      ISchemaFile file,
      LockManager lockManager) {
    this.subtreeRoot = subtreeRoot;
    this.cacheManager = cacheManager;
    this.file = file;
    this.lockManager = lockManager;
  }

  public void flushVolatileNodes() throws MetadataException, IOException {
    try {
      file.writeMNode(this.subtreeRoot);
    } catch (MetadataException | IOException e) {
      logger.warn(
          "Error occurred during MTree flush, current node is {}",
          this.subtreeRoot.getFullPath(),
          e);
      cacheManager.updateCacheStatusAfterFlushFailure(this.subtreeRoot);
      throw e;
    }

    Deque<Iterator<ICachedMNode>> volatileSubtreeStack = new ArrayDeque<>();
    volatileSubtreeStack.push(
        cacheManager.updateCacheStatusAndRetrieveSubtreeAfterPersist(this.subtreeRoot));

    Iterator<ICachedMNode> subtreeIterator;
    ICachedMNode subtreeRoot;
    while (!volatileSubtreeStack.isEmpty()) {
      subtreeIterator = volatileSubtreeStack.peek();
      if (!subtreeIterator.hasNext()) {
        volatileSubtreeStack.pop();
        continue;
      }

      subtreeRoot = subtreeIterator.next();

      lockManager.writeLock(subtreeRoot);
      try {
        file.writeMNode(subtreeRoot);
      } catch (MetadataException | IOException e) {
        logger.warn(
            "Error occurred during MTree flush, current node is {}", subtreeRoot.getFullPath(), e);
        processNotFlushedSubtrees(subtreeRoot, volatileSubtreeStack);
        throw e;
      } finally {
        lockManager.writeUnlock(subtreeRoot);
      }

      volatileSubtreeStack.push(
          cacheManager.updateCacheStatusAndRetrieveSubtreeAfterPersist(subtreeRoot));
    }
  }

  private void processNotFlushedSubtrees(
      ICachedMNode currentNode, Deque<Iterator<ICachedMNode>> volatileSubtreeStack) {
    cacheManager.updateCacheStatusAfterFlushFailure(currentNode);
    Iterator<ICachedMNode> subtreeIterator;
    while (!volatileSubtreeStack.isEmpty()) {
      subtreeIterator = volatileSubtreeStack.pop();
      while (subtreeIterator.hasNext()) {
        cacheManager.updateCacheStatusAfterFlushFailure(subtreeIterator.next());
      }
    }
  }
}
