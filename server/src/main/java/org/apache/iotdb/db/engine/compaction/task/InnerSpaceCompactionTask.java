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

import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(InnerSpaceCompactionTask.class);
  protected List<TsFileResource> selectedTsFileResourceList;
  protected TsFileResourceList tsFileResourceList;
  protected boolean sequence;
  protected String logicalStorageGroup;
  private TsFileResourceManager resourceManager;
  public static final String fileNameRegex = "([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)";

  public InnerSpaceCompactionTask(
      TsFileResourceManager resourceManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      Boolean sequence,
      String logicalStorageGroup) {
    this.resourceManager = resourceManager;
    this.tsFileResourceList = tsFileResourceList;
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    this.logicalStorageGroup = logicalStorageGroup;
  }

  @Override
  protected void doCompaction() throws Exception {
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    String targetFileName = generateTargetFileName(selectedTsFileResourceList);
    TsFileResource targetTsFileResource =
        new TsFileResource(new File(dataDirectory + File.separator + targetFileName));

    // transfer List<TsFileResourceListNode> to List<TsFileResource>
    List<TsFileResource> sourceFiles = new ArrayList<>();
    for (TsFileResource tsFileResource : selectedTsFileResourceList) {
      tsFileResource.readLock();
      tsFileResource.setMerging(true);
      LOGGER.info(
          "{} [Compaction] start to compact TsFile {}", logicalStorageGroup, tsFileResource);
    }
    try {
      File logFile = new File(dataDirectory + File.separator + targetFileName + ".log");
      // compaction execution
      List<Modification> modifications = new ArrayList<>();
      CompactionUtils.compact(
          targetTsFileResource,
          sourceFiles,
          logicalStorageGroup,
          new CompactionLogger(logFile.getPath()),
          new HashSet<>(),
          sequence,
          modifications);
    } finally {
      for (TsFileResource resource : sourceFiles) {
        resource.readUnlock();
      }
    }
    LOGGER.info(
        "{} [Compaction] compaction finish, start to delete old files", logicalStorageGroup);

    // TODO: clean the old file, add the new file to the list
  }

  public static String generateTargetFileName(List<TsFileResource> tsFileResourceList) {
    long minTimestamp = Long.MAX_VALUE;
    long minVersionNum = Long.MAX_VALUE;
    int maxInnerMergeTimes = Integer.MIN_VALUE;
    int maxCrossMergeTimes = Integer.MIN_VALUE;
    Pattern tsFilePattern = Pattern.compile(fileNameRegex);

    for (TsFileResource resource : tsFileResourceList) {
      String tsFileName = resource.getTsFile().getName();
      Matcher matcher = tsFilePattern.matcher(tsFileName);
      if (matcher.find()) {
        long currentTimestamp = Long.parseLong(matcher.group(1));
        long currentVersionNum = Long.parseLong(matcher.group(2));
        int currentInnerMergeTimes = Integer.parseInt(matcher.group(3));
        int currentCrossMergeTimes = Integer.parseInt(matcher.group(4));
        minTimestamp = Math.min(minTimestamp, currentTimestamp);
        minVersionNum = Math.min(minVersionNum, currentVersionNum);
        maxInnerMergeTimes = Math.max(maxInnerMergeTimes, currentInnerMergeTimes);
        maxCrossMergeTimes = Math.max(maxCrossMergeTimes, currentCrossMergeTimes);
      }
    }

    return TsFileNameGenerator.generateNewTsFileName(
        minTimestamp, minVersionNum, maxInnerMergeTimes, maxCrossMergeTimes);
  }
}
