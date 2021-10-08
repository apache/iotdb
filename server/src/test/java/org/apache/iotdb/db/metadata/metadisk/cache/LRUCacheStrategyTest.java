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
package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class LRUCacheStrategyTest {

  @Test
  public void testLRUEviction() {
    IMNode root = getSimpleTree();
    LRUCacheStrategy lruEviction = new LRUCacheStrategy();
    lruEviction.updateCacheStatus(root);
    lruEviction.updateCacheStatus(root.getChild("s1"));
    lruEviction.updateCacheStatus(root.getChild("s2"));
    lruEviction.updateCacheStatus(root.getChild("s1").getChild("t2"));
    StringBuilder stringBuilder = new StringBuilder();
    LRUCacheEntry entry = (LRUCacheEntry) root.getCacheEntry();
    while (entry != null) {
      stringBuilder.append(entry.getMNode().getFullPath()).append("\r\n");
      entry = entry.getPre();
    }
    Assert.assertEquals(
        "root\r\n" + "root.s1\r\n" + "root.s2\r\n" + "root.s1.t2\r\n", stringBuilder.toString());

    lruEviction.remove(root.getChild("s1"));
    stringBuilder = new StringBuilder();
    entry = (LRUCacheEntry) root.getCacheEntry();
    while (entry != null) {
      stringBuilder.append(entry.getMNode().getFullPath()).append("\r\n");
      entry = entry.getPre();
    }
    Assert.assertEquals("root\r\n" + "root.s2\r\n", stringBuilder.toString());

    Collection<IMNode> collection = lruEviction.evict();
    Assert.assertTrue(collection.contains(root));
    Assert.assertTrue(collection.contains(root.getChild("s2")));
    Assert.assertFalse(collection.contains(root.getChild("s1")));
  }

  private IMNode getSimpleTree() {
    IMNode root = new InternalMNode(null, "root");
    root.addChild("s1", new InternalMNode(root, "s1"));
    root.addChild("s2", new InternalMNode(root, "s2"));
    root.getChild("s1").addChild("t1", new InternalMNode(root.getChild("s1"), "t1"));
    root.getChild("s1").addChild("t2", new InternalMNode(root.getChild("s1"), "t2"));
    root.getChild("s1")
        .getChild("t2")
        .addChild("z1", new InternalMNode(root.getChild("s1").getChild("t2"), "z1"));
    root.getChild("s2").addChild("t1", new InternalMNode(root.getChild("s2"), "t1"));
    root.getChild("s2").addChild("t2", new InternalMNode(root.getChild("s2"), "t2"));
    return root;
  }
}
