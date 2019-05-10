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
import org.apache.iotdb.cluster.service.ClusterMonitorMBean;
import org.apache.iotdb.cluster.service.nodetool.NodeTool.NodeToolCmd;

@Command(name = "storagegroup", description = "Print all hosts information of specific storage group")
public class StorageGroup extends NodeToolCmd {

  @Option(title = "storage group", name = {"-sg",
      "--storagegroup"}, description = "Specify a storage group for accurate hosts information")
  private String sg = null;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    String nodes = proxy.getDataPartitionOfSG(sg);
    System.out.println(nodes);
  }
}
