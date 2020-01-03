/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;

public class TestUtils {
  private TestUtils() {
    // util class
  }

  public static Node getNode(int nodeNum) {
    Node node = new Node();
    node.setIp("192.168.0." + nodeNum);
    node.setMetaPort(ClusterDescriptor.getINSTANCE().getConfig().getLocalMetaPort());
    node.setDataPort(ClusterDescriptor.getINSTANCE().getConfig().getLocalDataPort());
    node.setNodeIdentifier(nodeNum);
    return node;
  }
}
