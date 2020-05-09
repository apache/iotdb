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

import java.util.List;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotTsFileFilter implements TsFileFilter {

  private static final Logger logger = LoggerFactory.getLogger(SlotTsFileFilter.class);
  private List<Integer> slots;

  public SlotTsFileFilter(List<Integer> slots) {
    this.slots = slots;
  }

  @Override
  public boolean fileNotSatisfy(TsFileResource resource) {
    return fileNotInSlots(resource, slots);
  }

  public static boolean fileNotInSlots(TsFileResource res, List<Integer> nodeSlots) {
    // {storageGroupName}/{partitionNum}/{fileName}
    String[] pathSegments = FilePathUtils.splitTsFilePath(res);
    String storageGroupName = pathSegments[pathSegments.length - 3];
    int partitionNum = Integer.parseInt(pathSegments[pathSegments.length - 2]);
    int slot = PartitionUtils.calculateStorageGroupSlotByPartition(storageGroupName, partitionNum,
        ClusterConstant.SLOT_NUM);
    boolean contained = nodeSlots.contains(slot);
    logger.debug("The slot of {} is {}, contained: {}", res.getFile().getPath(), slot, contained);
    return !contained;
  }
}
