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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.buffer;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache.CacheEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Iterator;

/**
 * NodeBuffer is used to hold volatile nodes in memory and help quickly locate the target nodes tobe
 * flushed. This interface defines the behaviours that an implementation shall meet.
 */
public interface INodeBuffer {

  IDatabaseMNode<ICachedMNode> getUpdatedDatabaseMNode();

  void updateDatabaseNodeAfterStatusUpdate(IDatabaseMNode<ICachedMNode> updatedDatabaseMNode);

  void removeUpdatedDatabaseNode();

  void addNewNodeToBuffer(ICachedMNode node);

  void addUpdatedNodeToBuffer(ICachedMNode node);

  void addBackToBufferAfterFlushFailure(ICachedMNode subTreeRoot);

  void remove(CacheEntry cacheEntry);

  long getBufferNodeNum();

  void clear();

  Iterator<ICachedMNode> iterator();
}
