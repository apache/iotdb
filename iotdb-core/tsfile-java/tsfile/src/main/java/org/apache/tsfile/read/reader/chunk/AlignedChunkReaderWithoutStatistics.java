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

package org.apache.tsfile.read.reader.chunk;

import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.util.List;

public class AlignedChunkReaderWithoutStatistics extends AlignedChunkReader {
  public AlignedChunkReaderWithoutStatistics(
      final Chunk timeChunk, final List<Chunk> valueChunkList) throws IOException {
    super(timeChunk, valueChunkList);
  }

  @Override
  protected boolean isEarlierThanReadStopTime(final PageHeader timePageHeader) {
    return false;
  }

  @Override
  protected boolean pageDeleted(
      final PageHeader pageHeader, final List<TimeRange> deleteIntervals) {
    return false;
  }
}
