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

package org.apache.iotdb.cluster.partition;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.iotdb.cluster.rpc.thrift.Node;

/**
 * PartitionGroup contains all the nodes that will form a data group with a certain node, which are
 * the REPLICATION_NUM - 1 different physical nodes after this node.
 * The first element of the list is called header, which is also the identifier of the data group.
 */
public class PartitionGroup extends ArrayList<Node> {

  private Node thisNode;

  public PartitionGroup() {
  }

  public PartitionGroup(Node... nodes) {
    this.addAll(Arrays.asList(nodes));
  }

  public PartitionGroup(PartitionGroup other) {
    super(other);
    this.thisNode = other.thisNode;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  public Node getHeader() {
    return get(0);
  }

  public void setThisNode(Node thisNode) {
    this.thisNode = thisNode;
  }

}
