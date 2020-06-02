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
package org.apache.iotdb.db.engine.merge;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.merge.utils.MergeMemCalculator;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFileSelector implements IMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(BaseFileSelector.class);

  protected long memoryBudget;
  protected long timeLimit;

  protected SelectorContext selectorContext;
  protected MergeResource resource;
  protected MergeMemCalculator memCalculator;
  protected List<TsFileResource> seqFiles;
  protected List<TsFileResource> unseqFiles;

  protected BaseFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget, long timeLowerBound) {
    this.selectorContext = new SelectorContext();
    this.resource = new MergeResource();
    this.memCalculator = new MergeMemCalculator(this.resource);
    this.memoryBudget = budget;
    this.seqFiles = seqFiles.stream().filter(
        tsFileResource -> MergeFileSelectorUtils.filterResource(tsFileResource, timeLowerBound))
        .collect(Collectors.toList());
    this.unseqFiles = unseqFiles.stream().filter(
        tsFileResource -> MergeFileSelectorUtils.filterResource(tsFileResource, timeLowerBound))
        .collect(Collectors.toList());
  }

  public Pair<MergeResource, SelectorContext> select() throws MergeException {
    selectorContext.setStartTime(System.currentTimeMillis());
    MergeResource resource = new MergeResource();
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles", seqFiles.size(),
          unseqFiles.size());

      Pair<List<TsFileResource>, List<TsFileResource>> selectedFiles = select(false);
      if (selectedFiles.right.isEmpty()) {
        selectedFiles = select(true);
      }
      resource.setSeqFiles(selectedFiles.left);
      resource.setUnseqFiles(selectedFiles.right);
      resource.removeOutdatedSeqReaders();
      if (resource.getUnseqFiles().isEmpty()) {
        logger.info("No merge candidates are found");
        return new Pair<>(resource, selectorContext);
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size(),
          selectorContext.getTotalCost(),
          System.currentTimeMillis() - selectorContext.getStartTime());
    }
    return new Pair<>(resource, selectorContext);
  }

  long getTotalCost() {
    return selectorContext.getTotalCost();
  }

  protected abstract Pair<List<TsFileResource>, List<TsFileResource>> select(boolean useTightBound)
      throws IOException;
}
