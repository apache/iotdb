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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SlotTsFileFilter implements TsFileFilter {

  private static final Logger logger = LoggerFactory.getLogger(SlotTsFileFilter.class);
  private Set<Integer> slots;

  public SlotTsFileFilter(Set<Integer> slots) {
    this.slots = slots;
  }

  public SlotTsFileFilter(List<Integer> slots) {
    this.slots = new HashSet<>(slots);
  }

  @Override
  public boolean fileNotSatisfy(TsFileResource resource) {
    return fileNotInSlots(resource, slots);
  }

  private static boolean fileNotInSlots(TsFileResource resource, Set<Integer> nodeSlots) {
    Pair<String, Long> sgNameAndPartitionIdPair =
        FilePathUtils.getLogicalSgNameAndTimePartitionIdPair(
            resource.getTsFile().getAbsolutePath());
    int slot =
        SlotPartitionTable.getSlotStrategy()
            .calculateSlotByPartitionNum(
                sgNameAndPartitionIdPair.left,
                sgNameAndPartitionIdPair.right,
                ClusterConstant.SLOT_NUM);
    boolean contained = nodeSlots.contains(slot);
    logger.debug(
        "The slot of {} is {}, contained: {}", resource.getTsFile().getPath(), slot, contained);
    return !contained;
  }
}
