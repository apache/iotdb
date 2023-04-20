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

import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

public class EncodeSeriesTask implements Task {
  private IDeviceID deviceId;
  private String seriesId;
  private FlushDeviceContext deviceContext;
  private FlushContext allContext;
  private IWritableMemChunk series;
  private IChunkWriter chunkWriter;

  public EncodeSeriesTask(
      IDeviceID deviceId,
      String seriesId,
      FlushDeviceContext deviceContext,
      FlushContext allContext,
      IWritableMemChunk series) {
    this.deviceId = deviceId;
    this.seriesId = seriesId;
    this.deviceContext = deviceContext;
    this.allContext = allContext;
    this.series = series;
  }

  @Override
  public DeviceIOTask nextTask() {
    Integer seriesIndex = deviceContext.getSeriesIndexMap().get(seriesId);
    deviceContext.getChunkWriters()[seriesIndex] = chunkWriter;
    int encodedSeriesNum = deviceContext.getEncodedCounter().incrementAndGet();
    if (encodedSeriesNum == deviceContext.getMeasurementIds().size()) {
      // the whole device has been encoded, try flushing it
      return new DeviceIOTask(allContext);
    }
    // some series are still under encoding, the last encoded one will trigger a DeviceIOTask
    return null;
  }

  @Override
  public void run() {
    long starTime = System.currentTimeMillis();
    chunkWriter = series.createIChunkWriter();
    series.encode(chunkWriter);
    chunkWriter.sealCurrentPage();
    chunkWriter.clearPageWriter();

    long subTaskTime = System.currentTimeMillis() - starTime;
    allContext
        .getWritingMetrics()
        .recordFlushSubTaskCost(WritingMetrics.ENCODING_TASK, subTaskTime);
    allContext.getEncodingTime().addAndGet(subTaskTime);
  }

  public IDeviceID getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(IDeviceID deviceId) {
    this.deviceId = deviceId;
  }

  public IWritableMemChunk getSeries() {
    return series;
  }

  public void setSeries(IWritableMemChunk series) {
    this.series = series;
  }
}
