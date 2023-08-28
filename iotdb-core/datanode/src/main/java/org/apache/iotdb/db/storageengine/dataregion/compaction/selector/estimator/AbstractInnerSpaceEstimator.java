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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.IOException;
import java.util.List;

/**
 * Estimate the memory cost of one inner space compaction task with specific source files based on
 * its corresponding implementation.
 */
public abstract class AbstractInnerSpaceEstimator extends AbstractCompactionEstimator {

  public long estimateInnerCompactionMemory(List<TsFileResource> resources) throws IOException {
    if (!config.isEnableCompactionMemControl()) {
      return 0;
    }
    CompactionTaskInfo taskInfo = calculatingCompactionTaskInfo(resources);
    long cost = calculatingMetadataMemoryCost(taskInfo);
    cost += calculatingDataMemoryCost(taskInfo);
    return cost;
  }
}
