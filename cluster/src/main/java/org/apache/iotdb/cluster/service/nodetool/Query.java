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

@Command(name = "query", description = "Print number of query jobs for all groups of connected host")
public class Query extends NodeToolCmd {

  @Override
  public void execute(ClusterMonitorMBean proxy)
  {
    Map<String, Integer> queryNumMap = proxy.getQueryJobNumMap();
    queryNumMap.forEach((groupId, num) -> System.out.println(groupId + "\t->\t" + num));
    int sum = queryNumMap.values().stream().mapToInt(num -> num).sum();
    System.out.println("Total\t->\t" + sum);
  }
}