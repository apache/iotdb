/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class ChunkReaderWithFilter extends ChunkReader {

  private Filter filter;

  public ChunkReaderWithFilter(Chunk chunk, Filter filter) {
    super(chunk, filter);
    this.filter = filter;
  }

  @Override
  public boolean pageSatisfied(PageHeader pageHeader) {
    if (pageHeader.getMax_timestamp() < getMaxTombstoneTime()) {
      return false;
    }
    DigestForFilter digest = new DigestForFilter(pageHeader.getMin_timestamp(),
        pageHeader.getMax_timestamp(),
        pageHeader.getStatistics().getMinBytebuffer(),
        pageHeader.getStatistics().getMaxBytebuffer(),
        chunkHeader.getDataType());
    return filter.satisfy(digest);
  }

}
