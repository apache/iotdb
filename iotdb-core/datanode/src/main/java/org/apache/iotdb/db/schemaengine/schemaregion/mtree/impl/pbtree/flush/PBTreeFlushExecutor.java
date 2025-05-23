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
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.IMemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer.getCachedMNodeContainer;

public class PBTreeFlushExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PBTreeFlushExecutor.class);

  private final Iterator<ICachedMNode> subtreeRoots;
  private final IDatabaseMNode<ICachedMNode> databaseMNode;
  private final AtomicInteger remainToFlush;

  private final IMemoryManager memoryManager;
  private final ISchemaFile file;
  private final LockManager lockManager;

  public PBTreeFlushExecutor(
      IMemoryManager memoryManager, ISchemaFile file, LockManager lockManager) {
    this.remainToFlush = null;
    this.subtreeRoots = memoryManager.collectVolatileSubtrees();
    this.databaseMNode = memoryManager.collectUpdatedStorageGroupMNodes();
    this.memoryManager = memoryManager;
    this.file = file;
    this.lockManager = lockManager;
  }

  public PBTreeFlushExecutor(
      AtomicInteger remainToFlush,
      IMemoryManager memoryManager,
      ISchemaFile file,
      LockManager lockManager) {
    this.remainToFlush = remainToFlush;
    this.subtreeRoots = memoryManager.collectVolatileSubtrees();
    this.databaseMNode = memoryManager.collectUpdatedStorageGroupMNodes();
    this.memoryManager = memoryManager;
    this.file = file;
    this.lockManager = lockManager;
  }

  public void flushVolatileNodes(AtomicLong flushNodeNum, AtomicLong flushMemSize)
      throws MetadataException {
    List<Exception> exceptions = new ArrayList<>();
    if (databaseMNode != null && checkRemainToFlush()) {
      try {
        processFlushDatabase(databaseMNode);
        flushNodeNum.incrementAndGet();
        flushMemSize.addAndGet(databaseMNode.estimateSize());
      } catch (Exception e) {
        exceptions.add(e);
        logger.warn(e.getMessage(), e);
      }
    }
    while (subtreeRoots.hasNext() && checkRemainToFlush()) {
      try {
        processFlushNonDatabase(subtreeRoots.next(), flushNodeNum, flushMemSize);
      } catch (Exception e) {
        exceptions.add(e);
        logger.warn(e.getMessage(), e);
      }
    }
    if (!exceptions.isEmpty()) {
      throw new MetadataException(
          exceptions.stream().map(Exception::getMessage).reduce("", (a, b) -> a + ", " + b));
    }
  }

  private boolean checkRemainToFlush() {
    if (remainToFlush == null) {
      return true;
    }
    return remainToFlush.decrementAndGet() >= 0;
  }

  private void processFlushDatabase(IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode)
      throws IOException {
    try {
      file.updateDatabaseNode(updatedStorageGroupMNode);
    } catch (IOException e) {
      logger.warn(
          "IOException occurred during updating StorageGroupMNode {}",
          updatedStorageGroupMNode.getFullPath(),
          e);
      throw e;
    }
  }

  private void processFlushNonDatabase(
      ICachedMNode subtreeRoot, AtomicLong flushNodeNum, AtomicLong flushMemSize)
      throws MetadataException, IOException {
    Iterator<ICachedMNode> volatileSubtreeIterator;
    List<ICachedMNode> collectedVolatileSubtrees;
    try {
      ICachedMNodeContainer container = getCachedMNodeContainer(subtreeRoot);
      container.transferAllBufferReceivingToFlushing();
      file.writeMNode(subtreeRoot);

      flushNodeNum.incrementAndGet();
      flushMemSize.addAndGet(subtreeRoot.estimateSize());
      volatileSubtreeIterator =
          memoryManager.updateCacheStatusAndRetrieveSubtreeAfterPersist(subtreeRoot);

      // make sure the roots in volatileSubtreeIterator have been locked
      collectedVolatileSubtrees = new ArrayList<>();
      while (volatileSubtreeIterator.hasNext()) {
        collectedVolatileSubtrees.add(volatileSubtreeIterator.next());
      }
    } catch (MetadataException | IOException e) {
      logger.warn(
          "Error occurred during MTree flush, current node is {}", subtreeRoot.getFullPath(), e);
      memoryManager.updateCacheStatusAfterFlushFailure(subtreeRoot);
      throw e;
    } finally {
      lockManager.writeUnlock(subtreeRoot);
    }

    Deque<Iterator<ICachedMNode>> volatileSubtreeStack = new ArrayDeque<>();
    volatileSubtreeStack.push(collectedVolatileSubtrees.iterator());

    Iterator<ICachedMNode> subtreeIterator;
    while (!volatileSubtreeStack.isEmpty()) {
      subtreeIterator = volatileSubtreeStack.peek();
      if (!subtreeIterator.hasNext()) {
        volatileSubtreeStack.pop();
        continue;
      }

      subtreeRoot = subtreeIterator.next();

      try {
        ICachedMNodeContainer container = getCachedMNodeContainer(subtreeRoot);
        container.transferAllBufferReceivingToFlushing();
        file.writeMNode(subtreeRoot);

        flushNodeNum.incrementAndGet();
        flushMemSize.addAndGet(subtreeRoot.estimateSize());
        collectedVolatileSubtrees = new ArrayList<>();
        volatileSubtreeIterator =
            memoryManager.updateCacheStatusAndRetrieveSubtreeAfterPersist(subtreeRoot);
        while (volatileSubtreeIterator.hasNext()) {
          collectedVolatileSubtrees.add(volatileSubtreeIterator.next());
        }
      } catch (MetadataException | IOException e) {
        logger.warn(
            "Error occurred during MTree flush, current node is {}", subtreeRoot.getFullPath(), e);
        processNotFlushedSubtrees(subtreeRoot, volatileSubtreeStack);
        throw e;
      } finally {
        lockManager.writeUnlock(subtreeRoot);
      }

      volatileSubtreeStack.push(collectedVolatileSubtrees.iterator());
    }
  }

  private void processNotFlushedSubtrees(
      ICachedMNode currentNode, Deque<Iterator<ICachedMNode>> volatileSubtreeStack) {
    memoryManager.updateCacheStatusAfterFlushFailure(currentNode);
    Iterator<ICachedMNode> subtreeIterator;
    ICachedMNode node;
    while (!volatileSubtreeStack.isEmpty()) {
      subtreeIterator = volatileSubtreeStack.pop();
      while (subtreeIterator.hasNext()) {
        node = subtreeIterator.next();
        memoryManager.updateCacheStatusAfterFlushFailure(node);
        lockManager.writeUnlock(node);
      }
    }
  }
}
