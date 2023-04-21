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
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.service.metrics.WritingMetrics;

/**
 * SortSeriesTask sorts a timeseries and generates the associated encoding task.
 */
public class SortSeriesTask implements Task {
  private IDeviceID deviceId;
  private String seriesId;
  private FlushDeviceContext deviceContext;
  private FlushContext allContext;
  private IWritableMemChunk series;

  public EncodeSeriesTask nextTask() {
    return new EncodeSeriesTask(deviceId, seriesId, deviceContext, allContext, series);
  }

  public void run() {
    long startTime = System.currentTimeMillis();
    /*
     * sort task (first task of flush pipeline)
     */
    series.sortTvListForFlush();
    long subTaskTime = System.currentTimeMillis() - startTime;
    allContext.getSortTime().addAndGet(subTaskTime);
    allContext.getWritingMetrics().recordFlushSubTaskCost(WritingMetrics.SORT_TASK, subTaskTime);
  }

  public IDeviceID getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(IDeviceID deviceId) {
    this.deviceId = deviceId;
  }

  public void setSeriesId(String seriesId) {
    this.seriesId = seriesId;
  }

  public void setDeviceContext(FlushDeviceContext deviceContext) {
    this.deviceContext = deviceContext;
  }

  public void setAllContext(FlushContext allContext) {
    this.allContext = allContext;
  }

  public IWritableMemChunk getSeries() {
    return series;
  }

  public void setSeries(IWritableMemChunk series) {
    this.series = series;
  }
}
