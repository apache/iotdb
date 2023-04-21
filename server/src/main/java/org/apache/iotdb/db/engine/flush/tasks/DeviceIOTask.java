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
package org.apache.iotdb.db.engine.flush.tasks;

import org.apache.iotdb.commons.concurrent.pipeline.Task;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import java.io.IOException;

/**
 * A DeviceIOTask will check if the current device in the FlushContext (indicated by
 * FlushContext.cursor) is fully encoded. If it is fully encoded, it will be flushed to the
 * underlying file and the cursor will be moved to the next device.
 * <p>
 * The above procedure repeats until the current device is still being encoded or all devices are
 * flushed. If all devices are flushed, a FinalTask will be generated. Otherwise, no new task will
 * be generated.
 */
public class DeviceIOTask implements Task {

  private FlushContext flushContext;

  public DeviceIOTask(FlushContext flushContext) {
    this.flushContext = flushContext;
  }

  @Override
  public void run() {
    boolean hasNext = true;
    while (hasNext) {
      int cursor = flushContext.getCursor();
      if (cursor < flushContext.getDeviceContexts().size()) {
        FlushDeviceContext flushDeviceContext = flushContext.getDeviceContexts().get(cursor);
        if (flushDeviceContext.isFullyEncoded()) {
          // the current device is ready, flush it and move to the next device
          flushOneDevice(flushDeviceContext);
          flushContext.setCursor(cursor + 1);
        } else {
          // the current device is still being flushed
          hasNext = false;
        }
      } else {
        // all devices are flushed
        hasNext = false;
      }
    }
  }

  public void flushOneDevice(FlushDeviceContext deviceContext) {
    long starTime = System.currentTimeMillis();
    try {
      flushContext.getWriter().startChunkGroup(deviceContext.getDeviceID().toStringID());
      for (IChunkWriter chunkWriter : deviceContext.getChunkWriters()) {
        chunkWriter.writeToFileWriter(flushContext.getWriter());
      }
      flushContext.getWriter().setMinPlanIndex(flushContext.getMemTable().getMinPlanIndex());
      flushContext.getWriter().setMaxPlanIndex(flushContext.getMemTable().getMaxPlanIndex());
      flushContext.getWriter().endChunkGroup();
    } catch (IOException e) {
      throw new FlushRunTimeException(e);
    }
    long subTaskTime = System.currentTimeMillis() - starTime;
    flushContext.getIoTime().addAndGet(subTaskTime);
    flushContext.getWritingMetrics().recordFlushSubTaskCost(WritingMetrics.IO_TASK, subTaskTime);
  }

  @Override
  public FinalTask nextTask() {
    if (flushContext.allFlushed()) {
      // all devices have been flushed
      return new FinalTask();
    }
    return null;
  }
}
