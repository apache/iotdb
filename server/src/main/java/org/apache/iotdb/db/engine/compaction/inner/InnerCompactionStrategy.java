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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

public enum InnerCompactionStrategy {
  SIZE_TIRED_COMPACTION;

  public AbstractInnerSpaceCompactionTask getCompactionTask(CompactionContext context) {
    switch (this) {
      case SIZE_TIRED_COMPACTION:
      default:
        return new SizeTiredCompactionTask(context);
    }
  }

  public AbstractInnerSpaceCompactionTask getCompactionRecoverTask(CompactionContext context) {
    switch (this) {
      case SIZE_TIRED_COMPACTION:
      default:
        return new SizeTiredCompactionRecoverTask(context);
    }
  }

  public AbstractInnerSpaceCompactionSelector getCompactionSelector(
      String storageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceList tsFileResources,
      boolean sequence,
      ICompactionTaskFactory taskFactory) {
    switch (this) {
      case SIZE_TIRED_COMPACTION:
      default:
        return new SizeTiredCompactionSelector(
            storageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileResources,
            sequence,
            taskFactory);
    }
  }
}
