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
import io.airlift.airline.Option;
import java.util.Map;
import org.apache.iotdb.cluster.service.ClusterMonitorMBean;
import org.apache.iotdb.cluster.service.nodetool.NodeTool.NodeToolCmd;

@Command(name = "ring", description = "Print information about the hash ring")
public class Ring extends NodeToolCmd {
  @Option(title = "physical_ring", name = {"-p", "--physical"}, description = "Show physical nodes instead of virtual ones")
  private boolean physical = false;

  @Override
  public void execute(ClusterMonitorMBean proxy)
  {
    Map<Integer, String> map = physical ? proxy.getPhysicalRing() : proxy.getVirtualRing();
    map.forEach((hash, ip) -> System.out.println(hash + "\t->\t" + ip));
  }
}