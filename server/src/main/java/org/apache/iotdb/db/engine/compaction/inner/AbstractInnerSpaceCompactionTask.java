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

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractInnerSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");

  protected List<TsFileResource> selectedTsFileResourceList;
  protected boolean sequence;
  protected long selectedFileSize;
  protected int sumOfCompactionCount;
  protected long maxFileVersion;

  public AbstractInnerSpaceCompactionTask(
          String storageGroupName, long timePartition, AtomicInteger currentTaskNum, boolean sequence, List<TsFileResource> selectedTsFileResourceList) {
    super(storageGroupName, timePartition, currentTaskNum);
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    collectSelectedFilesInfo();
  }

  private void collectSelectedFilesInfo() {
    selectedFileSize = 0L;
    sumOfCompactionCount = 0;
    maxFileVersion = -1L;
    for (TsFileResource resource : selectedTsFileResourceList) {
      try {
        selectedFileSize += resource.getTsFileSize();
        TsFileNameGenerator.TsFileName fileName = TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        sumOfCompactionCount += fileName.getInnerCompactionCnt();
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

}
