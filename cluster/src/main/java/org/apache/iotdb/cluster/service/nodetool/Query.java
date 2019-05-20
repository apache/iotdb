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
package org.apache.iotdb.cluster.service.nodetool;

import io.airlift.airline.Command;
import java.util.Map;
import org.apache.iotdb.cluster.service.nodetool.NodeTool.NodeToolCmd;
import org.apache.iotdb.cluster.service.ClusterMonitorMBean;

@Command(name = "query", description = "Print number of query jobs for all data partitions for all hosts")
public class Query extends NodeToolCmd {

  @Override
  public void execute(ClusterMonitorMBean proxy)
  {
    Map<String, Map<String, Integer>> queryNumMap = proxy.getQueryJobNumMap();
    queryNumMap.forEach((ip, map) -> {
      System.out.println(ip + ":");
      if (map != null) {
        map.forEach((groupId, num) -> System.out.println("\t" + groupId + "\t->\t" + num));
      }
    });
    final int[] sum = {0};
    queryNumMap.forEach((ip, map) -> {
      if (map != null) {
        map.forEach((groupId, num) -> sum[0] += num);
      }
    });
    System.out.println("Total\t->\t" + sum[0]);
  }
}