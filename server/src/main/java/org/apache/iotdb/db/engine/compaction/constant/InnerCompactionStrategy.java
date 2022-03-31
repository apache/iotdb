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

package org.apache.iotdb.db.engine.compaction.constant;

import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

public enum InnerCompactionStrategy {
  SIZE_TIERED_COMPACTION;

  public static InnerCompactionStrategy getInnerCompactionStrategy(String name) {
    if ("SIZE_TIERED_COMPACTION".equalsIgnoreCase(name)) {
      return SIZE_TIERED_COMPACTION;
    }
    throw new RuntimeException("Illegal Compaction Strategy " + name);
  }

  public AbstractInnerSpaceCompactionTask getCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroup,
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence) {
    switch (this) {
      case SIZE_TIERED_COMPACTION:
      default:
        return new SizeTieredCompactionTask(
            logicalStorageGroupName,
            virtualStorageGroup,
            timePartition,
            tsFileManager,
            selectedTsFileResourceList,
            sequence,
            CompactionTaskManager.currentTaskNum);
    }
  }

  public AbstractInnerSpaceCompactionSelector getCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      boolean sequence) {
    switch (this) {
      case SIZE_TIERED_COMPACTION:
      default:
        return new SizeTieredCompactionSelector(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileManager,
            sequence);
    }
  }
}
