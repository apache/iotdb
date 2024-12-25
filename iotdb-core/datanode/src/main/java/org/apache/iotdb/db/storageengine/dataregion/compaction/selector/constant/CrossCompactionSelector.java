/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant;

import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;

@SuppressWarnings("squid:S6548")
public enum CrossCompactionSelector {
  REWRITE;

  public static CrossCompactionSelector getCrossCompactionSelector(String name) {
    if (REWRITE.toString().equalsIgnoreCase(name)) {
      return REWRITE;
    }
    throw new IllegalCompactionSelectorNameException("Illegal Cross Compaction Selector " + name);
  }

  @SuppressWarnings("squid:S1301")
  public ICrossSpaceSelector createInstance(
      String logicalStorageGroupName,
      String virtualGroupId,
      long timePartition,
      TsFileManager tsFileManager,
      CompactionScheduleContext context) {
    switch (this) {
      case REWRITE:
      default:
        return new RewriteCrossSpaceCompactionSelector(
            logicalStorageGroupName, virtualGroupId, timePartition, tsFileManager, context);
    }
  }
}
