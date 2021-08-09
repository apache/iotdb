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

package org.apache.iotdb.db.engine.merge.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.StorageGroupCompactionTask;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CompactionMergeRecoverTask extends StorageGroupCompactionTask {

  private static final Logger logger = LoggerFactory.getLogger(CompactionMergeRecoverTask.class);

  private TsFileManagement.CompactionRecoverTask compactionRecoverTask;
  private RecoverMergeTask recoverMergeTask;
  private TsFileManagement tsFileManagement;
  private String storageGroupSysDir;

  public CompactionMergeRecoverTask(
      TsFileManagement tsFileManagement,
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      MergeCallback callback,
      String taskName,
      boolean fullMerge,
      String storageGroupName,
      StorageGroupProcessor.CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
    super(storageGroupName);
    this.tsFileManagement = tsFileManagement;
    this.compactionRecoverTask =
        this.tsFileManagement.new CompactionRecoverTask(closeCompactionMergeCallBack);
    this.storageGroupSysDir = storageGroupSysDir;
    this.recoverMergeTask =
        new RecoverMergeTask(
            seqFiles,
            unseqFiles,
            storageGroupSysDir,
            callback,
            taskName,
            fullMerge,
            storageGroupName);
  }

  @Override
  public Void call() throws Exception {
    tsFileManagement.recovering = true;
    try {
      try {
        recoverMergeTask.recoverMerge(
            IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
        File mergingMods =
            SystemFileFactory.INSTANCE.getFile(
                storageGroupSysDir, StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME);
        if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
          mergingMods.delete();
        }
      } catch (MetadataException | IOException e) {
        logger.error(e.getMessage(), e);
      }
      compactionRecoverTask.call();
    } finally {
      tsFileManagement.recovering = false;
    }
    return null;
  }
}
