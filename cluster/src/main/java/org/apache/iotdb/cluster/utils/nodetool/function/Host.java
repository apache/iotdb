/**
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
package org.apache.iotdb.cluster.utils.nodetool.function;

import static org.apache.iotdb.cluster.utils.nodetool.Printer.msgPrintln;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitorMBean;

@Command(name = "host", description = "Print partitions information which a specific host belongs to")
public class Host extends NodeToolCmd {

  @Option(title = "all nodes", name = {"-a", "--all"}, description = "Show all nodes partition info")
  private boolean showAll = false;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    Map<PartitionGroup, Integer> raftGroupMapSlotNum;
    if (showAll) {
      raftGroupMapSlotNum = proxy.getSlotNumOfAllNode();
    } else {
      raftGroupMapSlotNum = proxy.getSlotNumOfCurNode();
    }
    if (raftGroupMapSlotNum == null) {
      msgPrintln("The cluster is being created.");
      return;
    }
    showInfo(raftGroupMapSlotNum);
  }

  private void showInfo(Map<PartitionGroup, Integer> raftGroupMapSlotNum) {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("%-50s  %20s", "Raft group", "Slot Number"));
    msgPrintln(builder.toString());
    for (Entry<PartitionGroup, Integer> entry : raftGroupMapSlotNum.entrySet()) {
      builder = new StringBuilder();
      PartitionGroup raftGroup = entry.getKey();
      Integer slotNum = entry.getValue();
      builder.append('(');
      if (!raftGroup.isEmpty()) {
        builder.append(nodeToString(raftGroup.get(0)));
      }
      for (int i = 1; i < raftGroup.size(); i++) {
        builder.append(", ").append(nodeToString(raftGroup.get(i)));
      }
      builder.append(')');
      msgPrintln(String.format("%-50s->%20s", builder.toString(), slotNum));
    }
  }
}
