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
package org.apache.iotdb.cluster.utils.nodetool.function;

import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitorMBean;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.commons.collections4.map.MultiKeyMap;

import static org.apache.iotdb.cluster.utils.nodetool.Printer.msgPrintln;

@Command(
    name = "partition",
    description = "Print the hosts information of specific storage group and time range")
public class Partition extends NodeToolCmd {

  @Option(
      title = "path",
      required = true,
      name = {"-path", "--path"},
      description = "Specify a path for accurate hosts information")
  private String path = null;

  @Option(
      title = "start time",
      name = {"-st", "--starttime"},
      description = "Specify a start time for partition")
  private long startTime = System.currentTimeMillis();

  @Option(
      title = "end time",
      name = {"-et", "--endtime"},
      description = "Specify a end time for partition")
  private long endTime = System.currentTimeMillis();

  @Option(
      title = "metadata",
      name = {"-m", "--metadata"},
      description = "Query metadata")
  private boolean metadata = false;

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    if (endTime < startTime) {
      endTime = startTime;
    }
    if (metadata) {
      queryMetaPartition(proxy);
    } else {
      queryDataPartition(proxy);
    }
  }

  private void queryMetaPartition(ClusterMonitorMBean proxy) {
    PartitionGroup partitionGroup = proxy.getMetaPartition(path);
    if (partitionGroup == null) {
      msgPrintln(BUILDING_CLUSTER_INFO);
    } else if (partitionGroup.isEmpty()) {
      msgPrintln(String.format("The storage group of path <%s> doesn't exist.", path));
    } else {
      msgPrintln(String.format("META<%s>\t->\t%s", path, partitionGroupToString(partitionGroup)));
    }
  }

  private void queryDataPartition(ClusterMonitorMBean proxy) {
    MultiKeyMap<Long, PartitionGroup> timeRangeMapRaftGroup =
        proxy.getDataPartition(path, startTime, endTime);
    if (timeRangeMapRaftGroup == null) {
      msgPrintln(BUILDING_CLUSTER_INFO);
    } else if (timeRangeMapRaftGroup.isEmpty()) {
      msgPrintln(String.format("The storage group of path <%s> doesn't exist.", path));
    } else {
      timeRangeMapRaftGroup.forEach(
          (timeRange, raftGroup) ->
              msgPrintln(
                  String.format(
                      "DATA<%s, %s, %s>\t->\t%s",
                      path,
                      timeRange.getKey(0),
                      timeRange.getKey(1),
                      partitionGroupToString(raftGroup))));
    }
  }
}
