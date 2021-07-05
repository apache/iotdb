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

import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitorMBean;

import io.airlift.airline.Command;

import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.cluster.utils.nodetool.Printer.msgPrintln;

@Command(
    name = "migration",
    description =
        "Print information about how many slots are in the state of data migration for each data group. ")
public class Migration extends NodeToolCmd {

  @Override
  public void execute(ClusterMonitorMBean proxy) {
    try {
      Map<PartitionGroup, Integer> groupSlotsMap = proxy.getSlotNumInDataMigration();
      if (groupSlotsMap == null) {
        msgPrintln(FAIL_TO_GET_ALL_SLOT_STATUS_INFO);
        return;
      }
      if (groupSlotsMap.isEmpty()) {
        msgPrintln("No slots are in the state of data migration, users can change membership.");
      } else {
        msgPrintln(
            "Some slots are in the state of data migration, users can not change membership until the end of data migration:");
        msgPrintln(String.format("%-20s  %30s", "Slot num", "Data Group"));
        for (Entry<PartitionGroup, Integer> entry : groupSlotsMap.entrySet()) {
          PartitionGroup group = entry.getKey();
          msgPrintln(String.format("%-20d->%30s", entry.getValue(), partitionGroupToString(group)));
        }
      }
    } catch (LeaderUnknownException e) {
      msgPrintln(META_LEADER_UNKNOWN_INFO);
    } catch (Exception e) {
      msgPrintln(e.getMessage());
    }
  }
}
