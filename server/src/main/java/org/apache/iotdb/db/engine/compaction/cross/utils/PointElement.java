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
package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;

public class PointElement {
  public long timestamp;
  public int priority;
  public Pair<Long, Object> timeValuePair;
  public IBatchDataIterator page;
  public PageElement pageElement;

  public PointElement(PageElement pageElement) throws IOException {
    this.pageElement = pageElement;
    if (pageElement.iChunkReader instanceof ChunkReader) {
      this.page = pageElement.batchData.getTsBlockSingleColumnIterator();
    } else {
      this.page = pageElement.batchData.getTsBlockAlignedRowIterator();
    }
    this.timestamp = page.currentTime();
    this.timeValuePair = new Pair<>(timestamp, page.currentValue());
    this.priority = pageElement.priority;
  }

  public void updateTimeValuePair() {
    timeValuePair.left = page.currentTime();
    timeValuePair.right = page.currentValue();
  }

  public void setPoint(Long timestamp, Object value) {
    this.timeValuePair.left = timestamp;
    timeValuePair.right = value;
    this.timestamp = timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
