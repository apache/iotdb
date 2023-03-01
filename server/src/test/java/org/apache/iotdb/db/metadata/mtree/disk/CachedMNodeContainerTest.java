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
package org.apache.iotdb.db.metadata.mtree.disk;

import org.apache.iotdb.db.metadata.mnode.BasicMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.CachedMNodeContainer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CachedMNodeContainerTest {

  @Test
  public void testIterator() {
    CachedMNodeContainer container = new CachedMNodeContainer();
    Map<String, IMNode> childCache = new HashMap<>();
    childCache.put("1", new BasicMNode(null, "1"));
    childCache.put("2", new BasicMNode(null, "2"));
    childCache.put("5", new BasicMNode(null, "5"));
    container.loadChildrenFromDisk(childCache);
    container.put("3", new BasicMNode(null, "3"));
    container.put("4", new BasicMNode(null, "4"));
    container.put("6", new BasicMNode(null, "6"));
    container.updateMNode("5");
    container.updateMNode("6");
    Iterator<IMNode> iterator = container.getChildrenIterator();
    while (iterator.hasNext()) {
      System.out.println(iterator.next());
    }
  }
}
