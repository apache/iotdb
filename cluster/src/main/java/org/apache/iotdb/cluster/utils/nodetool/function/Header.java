/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.cluster.utils.nodetool.function;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitorMBean;
import org.apache.iotdb.tsfile.utils.Pair;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.util.List;

import static org.apache.iotdb.cluster.utils.nodetool.Printer.msgPrintln;

@Command(
    name = "header",
    description = "Print information about data group whose header is this node")
public class Header extends NodeToolCmd {

  @Option(
      title = "raft id",
      name = {"-id", "--raftid"},
      description =
          "Specify the raft id of data group whose header is this node, default value is 0")
  private int id = 0;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    try {
      List<Pair<Node, NodeCharacter>> allNodes = proxy.getDataGroup(id);
      if (allNodes == null) {
        msgPrintln(BUILDING_CLUSTER_INFO);
        return;
      }
      msgPrintln(String.format("%-20s  %30s", "Node Identifier", "Node"));
      for (Pair<Node, NodeCharacter> pair : allNodes) {
        Node node = pair.left;
        msgPrintln(
            String.format(
                "%-20d->%30s", node.nodeIdentifier, nodeCharacterToString(node, pair.right)));
      }
    } catch (Exception e) {
      msgPrintln(e.getMessage());
    }
  }
}
