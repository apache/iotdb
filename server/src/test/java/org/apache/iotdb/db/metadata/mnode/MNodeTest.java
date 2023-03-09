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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.newnode.IMemMNode;
import org.apache.iotdb.db.metadata.newnode.basic.BasicMNode;
import org.apache.iotdb.db.metadata.newnode.device.DeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.factory.IMNodeFactory;
import org.apache.iotdb.db.metadata.newnode.factory.MemMNodeFactory;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.MeasurementMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class MNodeTest {
  
  private final IMNodeFactory<IMemMNode> nodeFactory = new MemMNodeFactory();

  @Test
  public void testReplaceChild() {
    IMemMNode rootNode = nodeFactory.createInternalMNode(null, "root");

    IDeviceMNode<IMemMNode> aNode = new DeviceMNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode.getAsMNode());

    IMeasurementMNode<IMemMNode> bNode = new MeasurementMNode(aNode, "b", null, null);

    aNode.addChild(bNode.getName(), bNode.getAsMNode());
    aNode.addAlias("aliasOfb", bNode);

    IDeviceMNode<IMemMNode> newANode = new DeviceMNode(null, "a");
    rootNode.replaceChild(aNode.getName(), newANode.getAsMNode());

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    assertEquals("root.a.b", multiFullPaths.get(0));
    assertEquals("root.a.b", rootNode.getChild("a").getChild("aliasOfb").getFullPath());
    assertNotSame(aNode, rootNode.getChild("a"));
    assertSame(newANode, rootNode.getChild("a"));
  }

  @Test
  public void testAddChild() {
    IMemMNode rootNode = nodeFactory.createInternalMNode(null, "root");

    IMemMNode speedNode =
        rootNode
            .addChild(nodeFactory.createInternalMNode(null, "sg1"))
            .addChild(nodeFactory.createInternalMNode(null, "a"))
            .addChild(nodeFactory.createInternalMNode(null, "b"))
            .addChild(nodeFactory.createInternalMNode(null, "c"))
            .addChild(nodeFactory.createInternalMNode(null, "d"))
            .addChild(nodeFactory.createInternalMNode(null, "device"))
            .addChild(nodeFactory.createInternalMNode(null, "speed"));
    assertEquals("root.sg1.a.b.c.d.device.speed", speedNode.getFullPath());

    IMemMNode temperatureNode =
        rootNode
            .getChild("sg1")
            .addChild(nodeFactory.createInternalMNode(null, "aa"))
            .addChild(nodeFactory.createInternalMNode(null, "bb"))
            .addChild(nodeFactory.createInternalMNode(null, "cc"))
            .addChild(nodeFactory.createInternalMNode(null, "dd"))
            .addChild(nodeFactory.createInternalMNode(null, "device11"))
            .addChild(nodeFactory.createInternalMNode(null, "temperature"));
    assertEquals("root.sg1.aa.bb.cc.dd.device11.temperature", temperatureNode.getFullPath());
  }
}
