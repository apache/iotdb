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
import org.apache.iotdb.db.engine.compaction.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionSelector;
import org.apache.iotdb.db.engine.compaction.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.constant.InnerUnseqCompactionPerformer;

public class CompactionConfigRestorer {
  private boolean enableSeqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
  private boolean enableUnseqSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
  private boolean enableCrossSpaceCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
  private CrossCompactionSelector crossStrategy =
      IoTDBDescriptor.getInstance().getConfig().getCrossCompactionSelector();
  private InnerSequenceCompactionSelector innerStrategy =
      IoTDBDescriptor.getInstance().getConfig().getInnerSequenceCompactionSelector();
  private CompactionPriority priority =
      IoTDBDescriptor.getInstance().getConfig().getCompactionPriority();
  private long targetFileSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();
  private long targetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private long targetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
  private long chunkSizeLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();
  private long chunkPointNumLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();
  private int maxInnerCompactionCandidateFileNum =
      IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
  private int maxCrossCompactionCandidateFileNum =
      IoTDBDescriptor.getInstance().getConfig().getMaxCrossCompactionCandidateFileNum();
  private int concurrentCompactionThread =
      IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
  private long compactionScheduleIntervalInMs =
      IoTDBDescriptor.getInstance().getConfig().getCompactionScheduleIntervalInMs();
  private long compactionSubmissionIntervalInMs =
      IoTDBDescriptor.getInstance().getConfig().getCompactionSubmissionIntervalInMs();
  private int compactionWriteThroughputMbPerSec =
      IoTDBDescriptor.getInstance().getConfig().getCompactionWriteThroughputMbPerSec();

  private CrossCompactionPerformer oldCrossPerformer =
      IoTDBDescriptor.getInstance().getConfig().getCrossCompactionPerformer();
  private InnerSeqCompactionPerformer oldInnerSeqPerformer =
      IoTDBDescriptor.getInstance().getConfig().getInnerSeqCompactionPerformer();
  private InnerUnseqCompactionPerformer oldInnerUnseqPerformer =
      IoTDBDescriptor.getInstance().getConfig().getInnerUnseqCompactionPerformer();

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
    config.setMaxInnerCompactionCandidateFileNum(maxInnerCompactionCandidateFileNum);
    config.setMaxCrossCompactionCandidateFileNum(maxCrossCompactionCandidateFileNum);
    config.setCompactionThreadCount(concurrentCompactionThread);
    config.setCompactionScheduleIntervalInMs(compactionScheduleIntervalInMs);
    config.setCompactionSubmissionIntervalInMs(compactionSubmissionIntervalInMs);
    config.setCompactionWriteThroughputMbPerSec(compactionWriteThroughputMbPerSec);
    config.setCrossCompactionPerformer(oldCrossPerformer);
    config.setInnerSeqCompactionPerformer(oldInnerSeqPerformer);
    config.setInnerUnseqCompactionPerformer(oldInnerUnseqPerformer);
  }
}
