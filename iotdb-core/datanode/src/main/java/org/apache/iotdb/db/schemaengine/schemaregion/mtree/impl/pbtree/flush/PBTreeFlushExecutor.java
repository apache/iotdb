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
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.CachedMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.ICacheManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class PBTreeFlushExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PBTreeFlushExecutor.class);

  private final int schemaRegionId;

  private final CachedMTreeStore store;

  private final ICacheManager cacheManager;

  private final ISchemaFile file;

  private final Runnable flushCallback;

  public PBTreeFlushExecutor(
      int schemaRegionId,
      CachedMTreeStore store,
      ICacheManager cacheManager,
      ISchemaFile file,
      Runnable flushCallback) {
    this.schemaRegionId = schemaRegionId;
    this.store = store;
    this.cacheManager = cacheManager;
    this.file = file;
    this.flushCallback = flushCallback;
  }

  public void flushVolatileNodes() {
    try {
      if (flushVolatileDBNode() || flushVolatileSubtree()) {
        flushCallback.run();
      }
    } catch (MetadataException | IOException e) {
      logger.warn(
          "Exception occurred during MTree flush, current SchemaRegionId is {}", schemaRegionId, e);
    } catch (Throwable e) {
      logger.error(
          "Error occurred during MTree flush, current SchemaRegionId is {}", schemaRegionId, e);
      e.printStackTrace();
    }
  }

  private boolean flushVolatileDBNode() throws IOException {
    IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode =
        cacheManager.collectUpdatedStorageGroupMNodes();
    if (updatedStorageGroupMNode == null) {
      return false;
    }

    try {
      file.updateDatabaseNode(updatedStorageGroupMNode);
      return true;
    } catch (IOException e) {
      logger.warn(
          "IOException occurred during updating StorageGroupMNode {}",
          updatedStorageGroupMNode.getFullPath(),
          e);
      throw e;
    }
  }

  private boolean flushVolatileSubtree() throws MetadataException, IOException {
    long startTime = System.currentTimeMillis();
    Iterator<ICachedMNode> nodesToPersist = cacheManager.collectVolatileMNodes();
    boolean hasNodesToPersist = nodesToPersist.hasNext();

    ICachedMNode volatileNode;
    while (nodesToPersist.hasNext()) {
      volatileNode = nodesToPersist.next();
      try {
        file.writeMNode(volatileNode);
      } catch (MetadataException | IOException e) {
        logger.warn(
            "Error occurred during MTree flush, current node is {}", volatileNode.getFullPath(), e);
        throw e;
      }
      cacheManager.updateCacheStatusAfterPersist(volatileNode);
    }
    logger.info(
        "It takes {}ms to flush MTree in SchemaRegion {}",
        (System.currentTimeMillis() - startTime),
        schemaRegionId);
    return hasNodesToPersist;
  }
}
