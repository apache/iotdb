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

package org.apache.iotdb.cluster.query.filter;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;

import java.util.List;

public class SlotSgFilter implements StorageGroupFilter {

  private List<Integer> slots;

  public SlotSgFilter(List<Integer> slots) {
    this.slots = slots;
  }

  @Override
  public boolean satisfy(String storageGroup) {
    return satisfy(storageGroup, slots);
  }

  private static boolean satisfy(String storageGroup, List<Integer> nodeSlots) {
    int slot =
        SlotPartitionTable.getSlotStrategy()
            .calculateSlotByPartitionNum(storageGroup, 0, ClusterConstant.SLOT_NUM);
    return nodeSlots.contains(slot);
  }
}
