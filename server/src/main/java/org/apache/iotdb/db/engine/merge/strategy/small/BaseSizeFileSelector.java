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

import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.merge.utils.MergeMemCalculator;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSizeFileSelector implements IMergeFileSelector {

  protected SizeBasedFileSelectorStrategy sizeBasedFileSelectorStrategy;
  private static final Logger logger = LoggerFactory.getLogger(
      BaseSizeFileSelector.class);
  protected long timeBlock;
  protected long memoryBudget;
  protected long timeLimit;

  protected SelectorContext selectorContext;
  protected MergeMemCalculator memCalculator;
  protected MergeResource resource;

  protected List<TsFileResource> seqFiles;
  protected int minChunkPointNum;
  private String storageGroupName;
  private File storageGroupSysDir;

  public BaseSizeFileSelector(Collection<TsFileResource> seqFiles, long dataTTL,
      String storageGroupName, File storageGroupSysDir,
      SizeBasedFileSelectorStrategy sizeBasedFileSelectorStrategy) {
    this.memoryBudget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
    long timeLowerBound = System.currentTimeMillis() - dataTTL;
    this.selectorContext = new SelectorContext();
    this.resource = new MergeResource();
    this.memCalculator = new MergeMemCalculator(this.resource);
    this.storageGroupName = storageGroupName;
    this.storageGroupSysDir = storageGroupSysDir;
    this.seqFiles = seqFiles.stream().filter(
        tsFileResource -> MergeFileSelectorUtils.filterResource(tsFileResource, timeLowerBound))
        .collect(Collectors.toList());
    timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    timeBlock = IoTDBDescriptor.getInstance().getConfig().getMergeFileTimeBlock();
    if (timeBlock < 0) {
      timeBlock = Long.MAX_VALUE;
    }
    minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
        .getChunkMergePointThreshold();
    this.sizeBasedFileSelectorStrategy = sizeBasedFileSelectorStrategy;
  }

  @Override
  public MergeResource selectMergedFiles() throws MergeException {
    if (seqFiles.isEmpty()) {
      logger.info("{} no files to be merged", storageGroupName);
      return null;
    }
    this.selectorContext.setStartTime(System.currentTimeMillis());
    this.selectorContext.clearTimeConsumption();
    try {
      logger.info("Selecting merge candidates from {} seqFile", seqFiles.size());
      List<TsFileResource> selectedSeqFiles = select();
      resource.setSeqFiles(selectedSeqFiles);
      resource.removeOutdatedSeqReaders();
      if (resource.getSeqFiles().isEmpty()) {
        logger.info("No merge candidates are found");
        return null;
      }
      if (logger.isInfoEnabled()) {
        logger.info("Selected merge candidates, {} seqFiles, total memory cost {}, "
                + "time consumption {}ms", resource.getSeqFiles().size(),
            selectorContext.getTotalCost(),
            System.currentTimeMillis() - selectorContext.getStartTime());
      }
      if (resource.getSeqFiles().size() == 0) {
        logger.info("{} cannot select merge candidates under the budget {}", storageGroupName,
            memoryBudget);
        return null;
      }
      // avoid pending tasks holds the metadata and streams
      resource.clear();
      resource.setTaskName(storageGroupName + "-" + System.currentTimeMillis());
      resource.setStorageGroupSysDir(this.storageGroupSysDir);
      resource.setMergingModification(new ModificationFile(
          storageGroupSysDir + File.separator + MERGING_MODIFICATION_FILE_NAME));
      // do not cache metadata until true candidates are chosen, or too much metadata will be
      // cached during selection
      resource.setCacheDeviceMeta(true);

      for (TsFileResource tsFileResource : resource.getSeqFiles()) {
        tsFileResource.setMerging(true);
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    return resource;
  }

  public List<TsFileResource> select() throws IOException {
    this.selectorContext.setStartTime(System.currentTimeMillis());
    this.selectorContext.clearTimeConsumption();
    this.selectorContext.clearTotalCost();
    int tmpStartIdx = -1;
    int tmpEndIdx = -1;
    int startIdx = -1;
    int endIdx = -1;
    int seqIndex = 0;
    while (seqIndex < seqFiles.size() && this.selectorContext.getTimeConsumption() < timeLimit) {
      TsFileResource seqFile = seqFiles.get(seqIndex);
      if (!UpgradeUtils.isNeedUpgrade(seqFile)) {
        if (isSmallFile(seqFile)) {
          if (tmpStartIdx == -1) {
            tmpStartIdx = seqIndex;
          }
          tmpEndIdx = seqIndex;
          logger.debug("Adding a new {} seqFile as candidates", seqFile);
        }
      }
      startIdx = tmpStartIdx;
      endIdx = tmpEndIdx;

      seqIndex++;
      this.selectorContext.updateTimeConsumption();
    }
    if (startIdx == -1 || endIdx == -1) {
      return new ArrayList<>();
    }
    return seqFiles.subList(startIdx, endIdx + 1);
  }

  protected boolean isSmallFile(TsFileResource seqFile)
      throws IOException {
    List<Path> paths = resource.getFileReader(seqFile).getAllPaths();
    for (Path currentPath : paths) {
      List<ChunkMetadata> chunkMetadataList;
      try {
        chunkMetadataList = resource
            .queryChunkMetadata(new PartialPath(currentPath.getFullPath()), seqFile);
      } catch (IllegalPathException e) {
        throw new IOException(e);
      }
      int cnt = 0;
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        if (cnt != chunkMetadataList.size() - 1 && !this.sizeBasedFileSelectorStrategy
            .isChunkEnoughLarge(chunkMetadata, minChunkPointNum, timeBlock)) {
          return true;
        }
        cnt++;
      }
    }
    return false;
  }

}
