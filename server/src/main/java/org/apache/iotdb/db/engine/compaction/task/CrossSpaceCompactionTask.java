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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.merge.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.merge.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class CrossSpaceCompactionTask extends AbstractCompactionTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrossSpaceCompactionTask.class);
  protected CompactionContext context;
  protected CrossSpaceMergeResource mergeResource;
  protected String storageGroupDir;
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unSeqTsFileResourceList;
  protected boolean sequence;

  public CrossSpaceCompactionTask(CompactionContext context) {
    super(context.getStorageGroupName(), context.getTimePartitionId());
    this.context = context;
    this.mergeResource = context.getMergeResource();
    this.storageGroupDir = context.getStorageGroupDir();
    this.seqTsFileResourceList = context.getSequenceFileResourceList();
    this.unSeqTsFileResourceList = context.getUnsequenceFileResourceList();
    this.selectedSeqTsFileResourceList = context.getSelectedSequenceFiles();
    this.selectedUnSeqTsFileResourceList = context.getSelectedUnsequenceFiles();
    this.sequence = context.isSequence();
  }

  @Override
  protected void doCompaction() throws Exception {
    String taskName = storageGroupName + "-" + System.currentTimeMillis();
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            mergeResource,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            context.getConcurrentMergeCount(),
            storageGroupName);
    mergeTask.call();
  }

  public void mergeEndAction(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    // todo: add
  }
}
