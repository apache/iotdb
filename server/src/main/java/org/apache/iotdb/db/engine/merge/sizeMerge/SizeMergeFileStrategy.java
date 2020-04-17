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

package org.apache.iotdb.db.engine.merge.sizeMerge;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.IRecoverMergeTask;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.independence.selector.IndependenceMaxFileSelector;
import org.apache.iotdb.db.engine.merge.sizeMerge.independence.task.IndependenceMergeTask;
import org.apache.iotdb.db.engine.merge.sizeMerge.independence.task.RecoverIndependenceMergeTask;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.selector.RegularizationMaxFileSelector;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RecoverRegularizationMergeTask;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public enum SizeMergeFileStrategy {
  REGULARIZATION,
  INDEPENDENCE;
  // TODO new strategies?

  public IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles, long budget,
      long timeLowerBound) {
    switch (this) {
      case INDEPENDENCE:
        return new IndependenceMaxFileSelector(seqFiles, budget, timeLowerBound);
      case REGULARIZATION:
      default:
        return new RegularizationMaxFileSelector(seqFiles, budget, timeLowerBound);
    }
  }

  public Callable<Void> getMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    switch (this) {
      case INDEPENDENCE:
        return new IndependenceMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
      case REGULARIZATION:
      default:
        return new RegularizationMergeTask(mergeResource, storageGroupSysDir, callback, taskName,
            storageGroupName);
    }
  }

  public IRecoverMergeTask getRecoverMergeTask(List<TsFileResource> seqTsFiles,
      List<TsFileResource> unseqTsFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    switch (this) {
      case INDEPENDENCE:
        return new RecoverIndependenceMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
      case REGULARIZATION:
      default:
        return new RecoverRegularizationMergeTask(seqTsFiles,
            unseqTsFiles, storageGroupSysDir, callback, taskName, storageGroupName);
    }
  }
}
