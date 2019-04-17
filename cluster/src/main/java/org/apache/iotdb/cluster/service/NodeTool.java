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
package org.apache.iotdb.cluster.service;

import java.util.Map;
import java.util.SortedMap;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;

public class NodeTool {

  public static void main(String... args)
  {
    ClusterMonitor monitor = ClusterMonitor.INSTANCE;
    if (args.length == 0) {
      SortedMap<Integer, PhysicalNode> physicalRing = monitor.getPhysicalRing();
      physicalRing.entrySet()
          .forEach(entry -> System.out.println(entry.getValue() + "\t-->\t" + entry.getKey()));
    } else if ("showleader".equals(args[0])) {
      if (args.length > 1) {
        String leader = monitor.getLeaderOfSG(args[1]);
        System.out.println(leader);
      } else {
        Map<String, String> groupIdLeaderMap = monitor.getAllLeaders();
      }
    }

    System.exit(0);
  }
}
