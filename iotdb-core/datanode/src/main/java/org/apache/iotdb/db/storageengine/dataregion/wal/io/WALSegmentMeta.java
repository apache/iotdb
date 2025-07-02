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

package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import java.util.List;
import java.util.Objects;

//      [header][segment]
//      ^
//      |
//      position
public class WALSegmentMeta {

  private final long position;

  private final long headerSize;

  private final long segmentSize;

  private final String logFile;

  private List<Integer> buffersSize;

  public WALSegmentMeta(
      final long position, final long headerSize, final long segmentSize, String logFile) {
    this.position = position;
    this.headerSize = headerSize;
    this.segmentSize = segmentSize;
    this.logFile = logFile;
  }

  public void setBuffersSize(List<Integer> buffersSize) {
    this.buffersSize = buffersSize;
  }

  public String getLogFile() {
    return logFile;
  }

  public long getPosition() {
    return position;
  }

  public long getHeaderSize() {
    return headerSize;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    WALSegmentMeta that = (WALSegmentMeta) o;
    return position == that.position
        && headerSize == that.headerSize
        && segmentSize == that.segmentSize
        && Objects.equals(logFile, that.logFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(position, headerSize, segmentSize, logFile);
  }
}
