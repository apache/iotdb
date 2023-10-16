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
package org.apache.iotdb.db.pipe.extractor.historical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeBatchTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Similar to the base class, but it batches several files as an event to enable further
 * optimization during the latter transfer.
 */
public class BatchedTsFileExtractor extends PipeHistoricalDataRegionTsFileExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedTsFileExtractor.class);
  private int maxBatchSize;

  public BatchedTsFileExtractor(int maxBatchSize) {
    this.maxBatchSize = Math.max(1, maxBatchSize);
  }

  @Override
  public synchronized Event supply() {
    if (pendingQueue == null) {
      return null;
    }
    TsFileResource resource = pendingQueue.poll();
    if (resource == null) {
      return null;
    }
    List<TsFileResource> tsFileResourceList = new ArrayList<>(maxBatchSize);
    tsFileResourceList.add(resource);
    while (!pendingQueue.isEmpty() && tsFileResourceList.size() < maxBatchSize) {
      tsFileResourceList.add(resource);
    }

    final PipeBatchTsFileInsertionEvent event = new PipeBatchTsFileInsertionEvent(
        tsFileResourceList,
        false,
        pipeTaskMeta,
        pattern,
        historicalDataExtractionStartTime,
        historicalDataExtractionEndTime,
        !(isTsFileResourceCoveredByTimeRange(tsFileResourceList.get(0))
            && isTsFileResourceCoveredByTimeRange(
            tsFileResourceList.get(tsFileResourceList.size() - 1))));

    event.increaseReferenceCount(BatchedTsFileExtractor.class.getName());

    for (TsFileResource res : tsFileResourceList) {
      try {
        PipeResourceManager.tsfile().unpinTsFileResource(res);
      } catch (IOException e) {
        LOGGER.warn(
            "Pipe: failed to unpin TsFileResource after creating event, original path: {}",
            resource.getTsFilePath());
      }
    }

    return event;
  }

}
