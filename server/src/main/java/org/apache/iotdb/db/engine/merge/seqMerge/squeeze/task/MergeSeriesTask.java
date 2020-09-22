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

package org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseMergeSeriesTask;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.MergeSmallFilesStrategy;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeSeriesTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;

class MergeSeriesTask extends BaseMergeSeriesTask {

  MergeSeriesTask(MergeContext context, String taskName, MergeLogger mergeLogger,
      MergeResource mergeResource, List<PartialPath> unmergedSeries, String storageGroupName) {
    super(context, taskName, mergeLogger, mergeResource, unmergedSeries, storageGroupName);
  }

  List<TsFileResource> mergeSeries() throws IOException {
    List<TsFileResource> newResources;
    MergeSmallFilesStrategy mergeSmallFilesStrategy = IoTDBDescriptor.getInstance().getConfig()
        .getMergeOverlappedFilesStrategy();
    switch (mergeSmallFilesStrategy) {
      case REGULARIZATION:
      default:
        RegularizationMergeSeriesTask regularizationMergeSeriesTask = new RegularizationMergeSeriesTask(
            mergeContext, taskName, mergeLogger,
            resource, unmergedSeries,storageGroupName);
        newResources = regularizationMergeSeriesTask.mergeSeries();
        break;
    }
    return newResources;
  }
}
