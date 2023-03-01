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

import org.apache.iotdb.db.metadata.newnode.device.AbstractDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.db.metadata.newnode.measurement.IMeasurementMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class MNodeTest {
  private static ExecutorService service;

  @Before
  public void setUp() throws Exception {
    service =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("replaceChild-%d").build());
  }

  @Test
  public void testReplaceChild() {
    BasicMNode rootNode = new BasicMNode(null, "root");

    IDeviceMNode aNode = new AbstractDeviceMNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode);

    IMeasurementMNode bNode = MeasurementMNode.getMeasurementMNode(aNode, "b", null, null);

    aNode.addChild(bNode.getName(), bNode);
    aNode.addAlias("aliasOfb", bNode);

    IDeviceMNode newANode = new AbstractDeviceMNode(null, "a");
    rootNode.replaceChild(aNode.getName(), newANode);

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    assertEquals("root.a.b", multiFullPaths.get(0));
    assertEquals("root.a.b", rootNode.getChild("a").getChild("aliasOfb").getFullPath());
    assertNotSame(aNode, rootNode.getChild("a"));
    assertSame(newANode, rootNode.getChild("a"));
  }

  @Test
  public void testAddChild() {
    BasicMNode rootNode = new BasicMNode(null, "root");

    IMNode speedNode =
        rootNode
            .addChild(new BasicMNode(null, "sg1"))
            .addChild(new BasicMNode(null, "a"))
            .addChild(new BasicMNode(null, "b"))
            .addChild(new BasicMNode(null, "c"))
            .addChild(new BasicMNode(null, "d"))
            .addChild(new BasicMNode(null, "device"))
            .addChild(new BasicMNode(null, "speed"));
    assertEquals("root.sg1.a.b.c.d.device.speed", speedNode.getFullPath());

    IMNode temperatureNode =
        rootNode
            .getChild("sg1")
            .addChild(new BasicMNode(null, "aa"))
            .addChild(new BasicMNode(null, "bb"))
            .addChild(new BasicMNode(null, "cc"))
            .addChild(new BasicMNode(null, "dd"))
            .addChild(new BasicMNode(null, "device11"))
            .addChild(new BasicMNode(null, "temperature"));
    assertEquals("root.sg1.aa.bb.cc.dd.device11.temperature", temperatureNode.getFullPath());
  }
}
