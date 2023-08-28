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

package org.apache.iotdb.consensus.natraft.utils;

import org.apache.iotdb.consensus.common.Peer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeUtils {

  public static List<Peer> computeAddedNodes(List<Peer> oldNodes, List<Peer> newNodes) {
    List<Peer> addedNode = new ArrayList<>();
    for (Peer newNode : newNodes) {
      if (!oldNodes.contains(newNode)) {
        addedNode.add(newNode);
      }
    }
    return addedNode;
  }

  public static Collection<Peer> unionNodes(List<Peer> currNodes, List<Peer> newNodes) {
    if (newNodes == null) {
      return currNodes;
    }
    Set<Peer> nodeUnion = new HashSet<>();
    nodeUnion.addAll(currNodes);
    nodeUnion.addAll(newNodes);
    return nodeUnion;
  }
}
