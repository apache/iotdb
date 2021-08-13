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

import org.apache.iotdb.db.metadata.MetaUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

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
  public void testReplaceChild() throws InterruptedException {
    // after replacing a with c, the timeseries root.a.b becomes root.c.b
    MNode rootNode = new MNode(null, "root");

    MNode aNode = new MNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode);

    MNode bNode = new MNode(aNode, "b");
    aNode.addChild(bNode.getName(), bNode);
    aNode.addAlias("aliasOfb", bNode);

    for (int i = 0; i < 500; i++) {
      service.submit(
          new Thread(() -> rootNode.replaceChild(aNode.getName(), new MNode(null, "c"))));
    }

    if (!service.isShutdown()) {
      service.shutdown();
      service.awaitTermination(30, TimeUnit.SECONDS);
    }

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    assertEquals("root.c.b", multiFullPaths.get(0));
    assertEquals("root.c.b", rootNode.getChild("c").getChild("aliasOfb").getFullPath());
  }

  @Test
  public void testAddChild() {
    MNode rootNode = new MNode(null, "root");

    MNode speedNode =
        rootNode
            .addChild(new MNode(null, "sg1"))
            .addChild(new MNode(null, "a"))
            .addChild(new MNode(null, "b"))
            .addChild(new MNode(null, "c"))
            .addChild(new MNode(null, "d"))
            .addChild(new MNode(null, "device"))
            .addChild(new MNode(null, "speed"));
    assertEquals("root.sg1.a.b.c.d.device.speed", speedNode.getFullPath());

    MNode temperatureNode =
        rootNode
            .getChild("sg1")
            .addChild(new MNode(null, "aa"))
            .addChild(new MNode(null, "bb"))
            .addChild(new MNode(null, "cc"))
            .addChild(new MNode(null, "dd"))
            .addChild(new MNode(null, "device11"))
            .addChild(new MNode(null, "temperature"));
    assertEquals("root.sg1.aa.bb.cc.dd.device11.temperature", temperatureNode.getFullPath());
  }
}
