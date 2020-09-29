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

package org.apache.iotdb.cluster.utils;

import java.util.Objects;
import org.apache.iotdb.cluster.rpc.thrift.Node;

/**
 * ClusterNode overrides hashcode() and equals() in Node to avoid duplicates in hash data structures
 * caused by identifier change.
 */
public class ClusterNode extends Node {

  public ClusterNode() {
  }

  public ClusterNode(String ip, int metaPort, int nodeIdentifier, int dataPort, int clientPort) {
    super(ip, metaPort, nodeIdentifier, dataPort, clientPort);
  }

  public ClusterNode(Node other) {
    super(other);
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof ClusterNode)) {
      return false;
    }
    return equals(((ClusterNode) that));
  }

  public boolean equals(ClusterNode that) {
    return Objects.equals(this.ip, that.ip) && this.dataPort == that.dataPort
        && this.metaPort == that.metaPort && this.clientPort == that.clientPort;
  }


  @Override
  public int hashCode() {
    return Objects.hash(ip, metaPort, dataPort, clientPort);
  }

  @Override
  public String toString() {
    return "ClusterNode{"
        + " ip='" + ip + '\''
        + ", metaPort=" + metaPort
        + ", nodeIdentifier=" + nodeIdentifier
        + ", dataPort=" + dataPort
        + ", clientPort=" + clientPort
        + "}";
  }
}
