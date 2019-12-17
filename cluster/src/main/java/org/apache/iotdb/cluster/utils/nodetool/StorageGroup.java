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
package org.apache.iotdb.cluster.utils.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.HashSet;
import java.util.Set;

@Command(name = "storagegroup", description = "Print all hosts information of specific storage group")
public class StorageGroup extends NodeToolCmd {

  @Option(title = "all storagegroup", name = {"-a", "--all"}, description = "Show hosts info of all storage groups")
  private boolean showAll = false;

  @Option(title = "storage group", name = {"-sg",
      "--storagegroup"}, description = "Specify a storage group for accurate hosts information")
  private String sg = null;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    Set<String> sgSet;
    if (showAll) {
      sgSet = proxy.getAllStorageGroupsLocally();
    } else {
      sgSet = new HashSet<>();
      sgSet.add(sg);
    }

    if (!showAll && sg == null) {
      System.out.println("Metadata\t->\t" + proxy.getDataPartitionOfSG(sg));
    } else {
      sgSet.forEach(sg -> System.out.println(sg + "\t->\t" + proxy.getDataPartitionOfSG(sg)));
    }
  }
}
