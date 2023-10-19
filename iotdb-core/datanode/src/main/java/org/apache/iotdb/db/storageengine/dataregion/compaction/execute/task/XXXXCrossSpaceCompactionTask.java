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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.XXXXCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.IOException;
import java.util.List;

public class XXXXCrossSpaceCompactionTask extends AbstractCompactionTask {

  public XXXXCrossSpaceCompactionTask(
    long timePartition,
    TsFileManager tsFileManager,
    XXXXCrossCompactionTaskResource taskResource,
    long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    selectedSeqFiles = taskResource.getSeqFiles();
    selectedUnseqFiles = taskResource.getUnseqFiles();

  }

  private TsFileResource previousSeqFile;
  private TsFileResource nextSeqFile;
  private TsFileResource unseqFile;

  private List<TsFileResource> selectedSeqFiles;
  private List<TsFileResource> selectedUnseqFiles;

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    return null;
  }

  @Override
  protected boolean doCompaction() {

    // 1. 日志记录任务相关的文件
    // 2. TsFileManager 中移动到顺序区(不能使用 keepOrderInsert，因为还没有改名，此时排序插入会出错, 可以直接使用 insertBefore 或
    // insertAfter )
    // 3. 源 TsFileResource 加写锁
    // 4. rename 源乱序 TsFileResource 到目标位置
    // 5. 释放写锁
    return false;
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof XXXXCrossSpaceCompactionTask)) {
      return false;
    }
    return false;
  }

  @Override
  public long getEstimatedMemoryCost() {
    return 0;
  }

  @Override
  public int getProcessedFileNum() {
    return 0;
  }

  @Override
  protected void createSummary() {

  }
}
