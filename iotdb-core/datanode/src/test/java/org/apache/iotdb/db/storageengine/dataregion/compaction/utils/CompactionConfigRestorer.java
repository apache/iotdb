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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.CrossCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerSequenceCompactionSelector;

public class CompactionConfigRestorer {
  private boolean enableSeqSpaceCompaction = true;
  private boolean enableUnseqSpaceCompaction = true;
  private boolean enableCrossSpaceCompaction = true;
  private CrossCompactionSelector crossStrategy = CrossCompactionSelector.REWRITE;
  private InnerSequenceCompactionSelector innerStrategy =
      InnerSequenceCompactionSelector.SIZE_TIERED_SINGLE_TARGET;
  private CompactionPriority priority = CompactionPriority.BALANCE;
  private long targetFileSize = 1073741824L;
  private long targetChunkSize = 1048576L;
  private long targetChunkPointNum = 100000L;
  private long chunkSizeLowerBoundInCompaction = 128L;
  private long chunkPointNumLowerBoundInCompaction = 100L;
  private int maxInnerCompactionCandidateFileNum = 30;
  private int maxCrossCompactionCandidateFileNum = 1000;
  private int concurrentCompactionThread = 10;
  private long compactionScheduleIntervalInMs = 60000L;
  private int compactionWriteThroughputMbPerSec = 8;

  private CrossCompactionPerformer oldCrossPerformer =
      IoTDBDescriptor.getInstance().getConfig().getCrossCompactionPerformer();
  private InnerSeqCompactionPerformer oldInnerSeqPerformer =
      IoTDBDescriptor.getInstance().getConfig().getInnerSeqCompactionPerformer();
  private InnerUnseqCompactionPerformer oldInnerUnseqPerformer =
      IoTDBDescriptor.getInstance().getConfig().getInnerUnseqCompactionPerformer();

  private int oldMinCrossCompactionUnseqLevel =
      IoTDBDescriptor.getInstance().getConfig().getMinCrossCompactionUnseqFileLevel();

  public CompactionConfigRestorer() {}

  public void restoreCompactionConfig() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    config.setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    config.setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    config.setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    config.setCrossCompactionSelector(crossStrategy);
    config.setInnerSequenceCompactionSelector(innerStrategy);
    config.setCompactionPriority(priority);
    config.setTargetCompactionFileSize(targetFileSize);
    config.setTargetChunkSize(targetChunkSize);
    config.setTargetChunkPointNum(targetChunkPointNum);
    config.setChunkSizeLowerBoundInCompaction(chunkSizeLowerBoundInCompaction);
    config.setChunkPointNumLowerBoundInCompaction(chunkPointNumLowerBoundInCompaction);
    config.setInnerCompactionCandidateFileNum(maxInnerCompactionCandidateFileNum);
    config.setFileLimitPerCrossTask(maxCrossCompactionCandidateFileNum);
    config.setCompactionThreadCount(concurrentCompactionThread);
    config.setCompactionScheduleIntervalInMs(compactionScheduleIntervalInMs);
    config.setCompactionWriteThroughputMbPerSec(compactionWriteThroughputMbPerSec);
    config.setCrossCompactionPerformer(oldCrossPerformer);
    config.setInnerSeqCompactionPerformer(oldInnerSeqPerformer);
    config.setInnerUnseqCompactionPerformer(oldInnerUnseqPerformer);
    config.setMinCrossCompactionUnseqFileLevel(oldMinCrossCompactionUnseqLevel);
  }
}
