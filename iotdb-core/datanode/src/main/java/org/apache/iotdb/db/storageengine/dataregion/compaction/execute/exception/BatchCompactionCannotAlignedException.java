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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchCompactionPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactChunkPlan;

import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;

public class BatchCompactionCannotAlignedException extends RuntimeException {
  public BatchCompactionCannotAlignedException(
      PageHeader pageHeader, CompactChunkPlan compactChunkPlan, int pageIndex) {
    super(
        String.format(
            "Current page %s cannot be aligned with time chunk %s, page index is %s",
            pageHeader, compactChunkPlan, pageIndex));
  }

  public BatchCompactionCannotAlignedException(
      Statistics pageStatistics, CompactChunkPlan compactChunkPlan, int pageIndex) {
    super(
        String.format(
            "Current page %s cannot be aligned with time chunk %s, page index is %s",
            pageStatistics, compactChunkPlan, pageIndex));
  }

  public BatchCompactionCannotAlignedException(
      IChunkMetadata chunkMetadata,
      CompactChunkPlan compactChunkPlan,
      BatchCompactionPlan batchCompactionPlan) {
    super(
        String.format(
            "Current chunk %s cannot be aligned with time chunk: %s, all time chunk in first batch is %s",
            chunkMetadata, compactChunkPlan, batchCompactionPlan));
  }
}
