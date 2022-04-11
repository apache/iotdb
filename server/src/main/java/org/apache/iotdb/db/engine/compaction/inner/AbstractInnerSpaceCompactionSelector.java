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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

public abstract class AbstractInnerSpaceCompactionSelector extends AbstractCompactionSelector {
  protected String logicalStorageGroupName;
  protected String dataRegionName;
  protected long timePartition;
  protected List<TsFileResource> tsFileResources;
  protected boolean sequence;
  protected InnerSpaceCompactionTaskFactory taskFactory;
  protected TsFileManager tsFileManager;

  public AbstractInnerSpaceCompactionSelector(
      String logicalStorageGroupName,
      String dataRegionName,
      long timePartition,
      TsFileManager tsFileManager,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.dataRegionName = dataRegionName;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.sequence = sequence;
    this.taskFactory = taskFactory;
    if (sequence) {
      tsFileResources = tsFileManager.getSequenceListByTimePartition(timePartition).getArrayList();
    } else {
      tsFileResources =
          tsFileManager.getUnsequenceListByTimePartition(timePartition).getArrayList();
    }
  }

  @Override
  public abstract void selectAndSubmit();
}
