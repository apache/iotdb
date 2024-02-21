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

package org.apache.iotdb.db.metadata.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.MNodeChildBuffer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.MNodeUpdateChildBuffer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MNodeChildBufferTest {

  private final IMNodeFactory<ICachedMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getCachedMNodeIMNodeFactory();

  @Test
  public void testMNodeChildBuffer() {
    ICachedMNode rootNode = nodeFactory.createInternalMNode(null, "root");

    ICachedMNode speedNode =
        rootNode
            .addChild(nodeFactory.createInternalMNode(null, "sg1"))
            .addChild(nodeFactory.createInternalMNode(null, "device"))
            .addChild(nodeFactory.createInternalMNode(null, "speed"));
    assertEquals("root.sg1.device.speed", speedNode.getFullPath());

    ICachedMNode temperatureNode =
        rootNode
            .getChild("sg1")
            .addChild(nodeFactory.createInternalMNode(null, "device11"))
            .addChild(nodeFactory.createInternalMNode(null, "temperature"));
    assertEquals("root.sg1.device11.temperature", temperatureNode.getFullPath());

    MNodeChildBuffer buffer = new MNodeUpdateChildBuffer();
    assertTrue(buffer.getReceivingBuffer().isEmpty());
    assertTrue(buffer.getFlushingBuffer().isEmpty());
    assertTrue(buffer.isEmpty());

    buffer.put("root.sg1.device.speed", speedNode);
    assertEquals(1, buffer.getReceivingBuffer().size());
    assertEquals(0, buffer.getFlushingBuffer().size());
    assertEquals(1, buffer.size());

    buffer.transferReceivingBufferToFlushingBuffer();
    assertEquals(0, buffer.getReceivingBuffer().size());
    assertEquals(1, buffer.getFlushingBuffer().size());
    assertEquals(1, buffer.size());

    buffer.put("root.sg1.device.speed", speedNode);
    assertEquals(1, buffer.getReceivingBuffer().size());
    assertEquals(1, buffer.getFlushingBuffer().size());
    assertEquals(1, buffer.size());

    buffer.put("root.sg1.device11.temperature", temperatureNode);
    // check containskey and containsValue
    assertTrue(buffer.containsKey("root.sg1.device.speed"));
    assertTrue(buffer.containsKey("root.sg1.device11.temperature"));
    assertTrue(buffer.containsValue(speedNode));
    assertTrue(buffer.containsValue(temperatureNode));
    // check keyset and values, entryset
    assertEquals(2, buffer.keySet().size());
    assertEquals(2, buffer.values().size());
    assertEquals(2, buffer.entrySet().size());
    // check iterator and foreach
    // get iterator
    Iterator<ICachedMNode> iterator = buffer.getMNodeChildBufferIterator();
    // check iterator
    assertTrue(iterator.hasNext());
    assertEquals(speedNode, iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals(temperatureNode, iterator.next());
    assertFalse(iterator.hasNext());

    // check get
    assertEquals(speedNode, buffer.get("root.sg1.device.speed"));
    assertEquals(temperatureNode, buffer.get("root.sg1.device11.temperature"));

    // check remove
    buffer.remove("root.sg1.device.speed");
    assertEquals(1, buffer.getReceivingBuffer().size());
    assertEquals(0, buffer.getFlushingBuffer().size());
    assertEquals(1, buffer.size());

    // check removeFromFlushingBuffer
    buffer.transferReceivingBufferToFlushingBuffer();
    buffer.putIfAbsent("root.sg1.device11.temperature", temperatureNode);
    buffer.removeFromFlushingBuffer("root.sg1.device11.temperature");
    assertEquals(1, buffer.getReceivingBuffer().size());
    assertEquals(0, buffer.getFlushingBuffer().size());
    assertEquals(1, buffer.size());

    // check clear
    buffer.transferReceivingBufferToFlushingBuffer();
    buffer.putIfAbsent("root.sg1.device11.temperature", temperatureNode);
    buffer.clear();
    assertTrue(buffer.getReceivingBuffer().isEmpty());
    assertTrue(buffer.getFlushingBuffer().isEmpty());
    assertTrue(buffer.isEmpty());
  }
}
