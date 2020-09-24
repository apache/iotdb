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

package org.apache.iotdb.db.engine.merge.strategy.small;

import java.util.List;
import org.apache.iotdb.db.engine.merge.BaseMergeSchedulerTask;
import org.apache.iotdb.db.engine.merge.FileMergeStrategy;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.strategy.small.regularization.task.RegularizationMergeSchedulerTask;
import org.apache.iotdb.db.engine.merge.strategy.small.regularization.task.RegularizationMergeTask;
import org.apache.iotdb.db.metadata.PartialPath;

public enum MergeSmallFilesStrategy implements FileMergeStrategy {
  REGULARIZATION;

  @Override
  public MergeTask getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    switch (this) {
      case REGULARIZATION:
      default:
        return new RegularizationMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
    }
  }

  @Override
  public BaseMergeSchedulerTask getMergeSchedulerTask(MergeContext context, String taskName,
      MergeLogger mergeLogger, MergeResource mergeResource, List<PartialPath> unmergedSeries,
      String storageGroupName) {
    switch (this) {
      case REGULARIZATION:
      default:
        return new RegularizationMergeSchedulerTask(context, taskName, mergeLogger, mergeResource,
            unmergedSeries, storageGroupName);
    }
  }
}
