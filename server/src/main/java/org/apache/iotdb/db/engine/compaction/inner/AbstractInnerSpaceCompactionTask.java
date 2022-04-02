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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractInnerSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  protected List<TsFileResource> selectedTsFileResourceList;
  protected boolean sequence;
  protected long selectedFileSize;
  protected int sumOfCompactionCount;
  protected long maxFileVersion;
  protected int maxCompactionCount;

  public AbstractInnerSpaceCompactionTask(
      String storageGroupName,
      long timePartition,
      AtomicInteger currentTaskNum,
      boolean sequence,
      List<TsFileResource> selectedTsFileResourceList,
      TsFileManager tsFileManager) {
    super(storageGroupName, timePartition, tsFileManager, currentTaskNum);
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    collectSelectedFilesInfo();
  }

  @Override
  public void setSourceFilesToCompactionCandidate() {
    this.selectedTsFileResourceList.forEach(
        tsFileResource -> {
          try {
            tsFileResource.setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE);
          } catch (Exception e) {
            LOGGER.error("Exception occurs when setting compaction candidate", e);
          }
        });
  }

  private void collectSelectedFilesInfo() {
    selectedFileSize = 0L;
    sumOfCompactionCount = 0;
    maxFileVersion = -1L;
    maxCompactionCount = -1;
    if (selectedTsFileResourceList == null) {
      return;
    }
    for (TsFileResource resource : selectedTsFileResourceList) {
      try {
        selectedFileSize += resource.getTsFileSize();
        TsFileNameGenerator.TsFileName fileName =
            TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        sumOfCompactionCount += fileName.getInnerCompactionCnt();
        if (fileName.getInnerCompactionCnt() > maxCompactionCount) {
          maxCompactionCount = fileName.getInnerCompactionCnt();
        }
        if (fileName.getVersion() > maxFileVersion) {
          maxFileVersion = fileName.getVersion();
        }
      } catch (IOException e) {
        LOGGER.warn("Fail to get the tsfile name of {}", resource.getTsFile(), e);
      }
    }
  }

  public List<TsFileResource> getSelectedTsFileResourceList() {
    return selectedTsFileResourceList;
  }

  public boolean isSequence() {
    return sequence;
  }

  public long getSelectedFileSize() {
    return selectedFileSize;
  }

  public int getSumOfCompactionCount() {
    return sumOfCompactionCount;
  }

  public long getMaxFileVersion() {
    return maxFileVersion;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    if (!tsFileManager.isAllowCompaction()) {
      return false;
    }

    for (TsFileResource resource : selectedTsFileResourceList) {
      if (resource.isCompacting() | !resource.isClosed() || !resource.getTsFile().exists()) {
        return false;
      }
    }

    for (TsFileResource resource : selectedTsFileResourceList) {
      resource.setStatus(TsFileResourceStatus.COMPACTING);
    }
    return true;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(fullStorageGroupName)
        .append("-")
        .append(timePartition)
        .append(" task file num is ")
        .append(selectedTsFileResourceList.size())
        .append(", files is ")
        .append(selectedTsFileResourceList)
        .append(", total compaction count is ")
        .append(sumOfCompactionCount)
        .toString();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public void resetCompactionCandidateStatusForAllSourceFiles() {
    selectedTsFileResourceList.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
  }
}
