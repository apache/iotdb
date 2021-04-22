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
package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TimePageReader {

  private final PageHeader pageHeader;

  /** decoder for time column */
  protected Decoder timeDecoder;

  /** time column in memory */
  protected ByteBuffer timeBuffer;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public TimePageReader(ByteBuffer pageData, Decoder timeDecoder) {
    this(null, pageData, timeDecoder);
  }

  public TimePageReader(PageHeader pageHeader, ByteBuffer pageData, Decoder timeDecoder) {
    this.timeDecoder = timeDecoder;
    this.pageHeader = pageHeader;
    this.timeBuffer = pageData;
  }

  public long[] nexTimeBatch() throws IOException {
    long[] timeBatch = new long[(int) pageHeader.getStatistics().getCount()];
    int index = 0;
    while (timeDecoder.hasNext(timeBuffer)) {
      timeBatch[index++] = timeDecoder.readLong(timeBuffer);
    }
    return timeBatch;
  }

  public TimeStatistics getStatistics() {
    return (TimeStatistics) pageHeader.getStatistics();
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public boolean isModified() {
    return pageHeader.isModified();
  }

  protected boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }
}
