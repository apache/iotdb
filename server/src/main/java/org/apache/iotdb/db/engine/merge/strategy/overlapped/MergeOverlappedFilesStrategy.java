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

package org.apache.iotdb.db.engine.merge.strategy.overlapped;

import java.util.List;
import org.apache.iotdb.db.engine.merge.BaseMergeSchedulerTask;
import org.apache.iotdb.db.engine.merge.FileMergeStrategy;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.task.InplaceAppendMergeTask;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.task.InplaceFullMergeTask;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.task.SqueezeAppendMergeTask;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.squeeze.task.SqueezeFullMergeTask;
import org.apache.iotdb.db.metadata.PartialPath;

public enum MergeOverlappedFilesStrategy implements FileMergeStrategy {
  INPLACE_FULL_MERGE,
  INPLACE_APPEND_MERGE,
  SQUEEZE_FULL_MERGE,
  SQUEEZE_APPEND_MERGE;

  @Override
  public MergeTask getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    switch (this) {
      case INPLACE_FULL_MERGE:
        return new InplaceFullMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
      case INPLACE_APPEND_MERGE:
        return new InplaceAppendMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
      case SQUEEZE_FULL_MERGE:
        return new SqueezeFullMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
      case SQUEEZE_APPEND_MERGE:
      default:
        return new SqueezeAppendMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
    }
  }

  @Override
  public BaseMergeSchedulerTask getMergeSchedulerTask(MergeContext context, String taskName,
      MergeLogger mergeLogger, MergeResource mergeResource, List<PartialPath> unmergedSeries,
      String storageGroupName) {
    return null;
  }
}
