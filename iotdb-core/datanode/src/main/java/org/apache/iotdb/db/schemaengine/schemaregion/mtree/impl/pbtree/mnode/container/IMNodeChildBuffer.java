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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Iterator;
import java.util.Map;

public interface IMNodeChildBuffer extends IMNodeContainer<ICachedMNode> {

  // get the iterator of the whole Buffer which is make a merge sort of the ReceivingBuffer and the
  // FlushingBuffer
  Iterator<ICachedMNode> getMNodeChildBufferIterator();

  // only get the FlushingBuffer,  there shall not be write operation on the returned map instance.
  Map<String, ICachedMNode> getFlushingBuffer();

  // only get the ReceivingBuffer, there shall not be write operation on the returned map instance.
  Map<String, ICachedMNode> getReceivingBuffer();

  // Before flushing, use this to transfer ReceivingBuffer to FlushingBuffer
  void transferReceivingBufferToFlushingBuffer();

  // After flushing, use this to clear the flushed node
  ICachedMNode removeFromFlushingBuffer(Object key);
}
