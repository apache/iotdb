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

import java.io.File;
import java.util.Collection;
import org.apache.iotdb.db.engine.merge.FileSelectorStrategy;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.strategy.small.regularization.selector.PointRangeFileSelector;
import org.apache.iotdb.db.engine.merge.strategy.small.regularization.selector.TimeRangeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

public enum SizeBasedFileSelectorStrategy implements FileSelectorStrategy {
  TIME_RANGE,
  POINT_RANGE;

  public boolean isChunkEnoughLarge(IChunkWriter chunkWriter, long minChunkPointNum, long minTime,
      long maxTime, long timeBlock) {
    switch (this) {
      case TIME_RANGE:
        return maxTime > 0 && maxTime - minTime >= timeBlock;
      case POINT_RANGE:
      default:
        return chunkWriter.getPtNum() >= minChunkPointNum;
    }
  }

  public boolean isChunkEnoughLarge(ChunkMetadata chunkMetadata, long minChunkPointNum,
      long timeBlock) {
    switch (this) {
      case TIME_RANGE:
        return chunkMetadata.getEndTime() - chunkMetadata.getStartTime() >= timeBlock;
      case POINT_RANGE:
      default:
        return chunkMetadata.getNumOfPoints() >= minChunkPointNum;
    }
  }

  @Override
  public IMergeFileSelector getFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long dataTTL, String storageGroupName,
      File storageGroupSysDir) {
    switch (this) {
      case POINT_RANGE:
        return new PointRangeFileSelector(seqFiles, dataTTL, storageGroupName, storageGroupSysDir);
      case TIME_RANGE:
      default:
        return new TimeRangeFileSelector(seqFiles, dataTTL, storageGroupName, storageGroupSysDir);
    }
  }
}
