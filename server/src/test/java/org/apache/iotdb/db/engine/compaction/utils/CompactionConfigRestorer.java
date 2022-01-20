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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.cross.CrossCompactionStrategy;
import org.apache.iotdb.db.engine.compaction.inner.InnerCompactionStrategy;

public class CompactionConfigRestorer {
  private boolean enableSeqSpaceCompaction = true;
  private boolean enableUnseqSpaceCompaction = false;
  private boolean enableCrossSpaceCompaction = true;
  private CrossCompactionStrategy crossStrategy = CrossCompactionStrategy.REWRITE_COMPACTION;
  private InnerCompactionStrategy innerStrategy = InnerCompactionStrategy.SIZE_TIERED_COMPACTION;
  private CompactionPriority priority = CompactionPriority.INNER_CROSS;
  private long targetFileSize = 1073741824L;
  private long targetChunkSize = 1048576L;
  private long targetChunkPointNum = 100000L;
  private long chunkSizeLowerBoundInCompaction = 128L;
  private long chunkPointNumLowerBoundInCompaction = 100L;
  private int maxCompactionCandidateFileNum = 30;
  private int maxOpenFileNumInCrossSpaceCompaction = 100;
  private int concurrentCompactionThread = 10;
  private long compactionScheduleIntervalInMs = 60000L;
  private long compactionSubmissionIntervalInMs = 60000L;
  private int compactionWriteThroughputMbPerSec = 8;

  public CompactionConfigRestorer() {}

  public void restoreCompactionConfig() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    config.setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    config.setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    config.setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    config.setCrossCompactionStrategy(crossStrategy);
    config.setInnerCompactionStrategy(innerStrategy);
    config.setCompactionPriority(priority);
    config.setTargetCompactionFileSize(targetFileSize);
    config.setTargetChunkSize(targetChunkSize);
    config.setTargetChunkPointNum(targetChunkPointNum);
    config.setChunkSizeLowerBoundInCompaction(chunkSizeLowerBoundInCompaction);
    config.setChunkPointNumLowerBoundInCompaction(chunkPointNumLowerBoundInCompaction);
    config.setMaxCompactionCandidateFileNum(maxCompactionCandidateFileNum);
    config.setMaxOpenFileNumInCrossSpaceCompaction(maxOpenFileNumInCrossSpaceCompaction);
    config.setConcurrentCompactionThread(concurrentCompactionThread);
    config.setCompactionScheduleIntervalInMs(compactionScheduleIntervalInMs);
    config.setCompactionSubmissionIntervalInMs(compactionSubmissionIntervalInMs);
    config.setCompactionWriteThroughputMbPerSec(compactionWriteThroughputMbPerSec);
  }
}
