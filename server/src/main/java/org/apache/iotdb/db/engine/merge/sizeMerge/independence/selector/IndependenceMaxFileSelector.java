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

package org.apache.iotdb.db.engine.merge.sizeMerge.independence.selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.merge.utils.MergeMemCalculator;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndependenceMaxFileSelector selects the most files from given seqFiles which can be merged as a
 * given time block with single device in a file
 */
public class IndependenceMaxFileSelector implements IMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(
      IndependenceMaxFileSelector.class);

  private long timeBlock;
  private long memoryBudget;
  private long timeLimit;

  private SelectorContext selectorContext;
  private MergeMemCalculator memCalculator;
  private MergeResource resource;

  private List<TsFileResource> seqFiles;

  public IndependenceMaxFileSelector(Collection<TsFileResource> seqFiles, long budget) {
    this(seqFiles, budget, Long.MIN_VALUE);
  }

  public IndependenceMaxFileSelector(Collection<TsFileResource> seqFiles, long budget,
      long timeLowerBound) {
    this.selectorContext = new SelectorContext();
    this.resource = new MergeResource();
    this.memCalculator = new MergeMemCalculator(this.resource);
    this.memoryBudget = budget;
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
  }

  @Override
  public Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException {
    return null;
  }

  List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }
}
