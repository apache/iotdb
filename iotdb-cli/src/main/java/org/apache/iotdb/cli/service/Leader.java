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
package org.apache.iotdb.cli.service;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.iotdb.cli.service.NodeTool.NodeToolCmd;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.service.ClusterMonitorMBean;

@Command(name = "leader", description = "Print leader host information of specific storage group")
public class Leader extends NodeToolCmd {

  @Arguments(description = "Specify a storage group for accurate leader information")
  private String sg = null;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    if (sg == null) {
      sg = ClusterConfig.METADATA_GROUP_ID;
    }
    String leader = proxy.getLeaderOfSG(sg);
    System.out.println(leader);
  }
}
