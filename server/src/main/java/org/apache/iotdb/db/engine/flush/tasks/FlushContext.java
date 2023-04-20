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

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FlushContext {
  private final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();
  private List<FlushDeviceContext> deviceContexts;
  private AtomicInteger cursor = new AtomicInteger();
  private RestorableTsFileIOWriter writer;
  private AtomicLong sortTime = new AtomicLong();
  private AtomicLong encodingTime = new AtomicLong();
  private AtomicLong ioTime = new AtomicLong();
  private IMemTable memTable;

  public List<FlushDeviceContext> getDeviceContexts() {
    return deviceContexts;
  }

  public void setDeviceContexts(List<FlushDeviceContext> deviceContexts) {
    this.deviceContexts = deviceContexts;
  }

  public int getCursor() {
    return cursor.get();
  }

  public void setCursor(int cursor) {
    this.cursor.set(cursor);
  }

  public WritingMetricsManager getWritingMetrics() {
    return WRITING_METRICS;
  }

  public AtomicLong getSortTime() {
    return sortTime;
  }

  public AtomicLong getEncodingTime() {
    return encodingTime;
  }

  public AtomicLong getIoTime() {
    return ioTime;
  }

  public RestorableTsFileIOWriter getWriter() {
    return writer;
  }

  public void setWriter(RestorableTsFileIOWriter writer) {
    this.writer = writer;
  }

  public IMemTable getMemTable() {
    return memTable;
  }

  public void setMemTable(IMemTable memTable) {
    this.memTable = memTable;
  }
}
