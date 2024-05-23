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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;

import java.io.IOException;

public class MemChunkHandleImpl implements IChunkHandle {
  protected final long[] dataOfTimestamp;

  protected boolean hasRead = false;

  public MemChunkHandleImpl(long[] dataOfTimestamp) {
    this.dataOfTimestamp = dataOfTimestamp;
  }

  @Override
  public boolean hasNextPage() throws IOException {
    return !hasRead;
  }

  // MemChunk only has one page in handle
  @Override
  public void skipCurrentPage() {
    hasRead = true;
  }

  @Override
  public long[] getPageStatisticsTime() {
    return new long[] {dataOfTimestamp[0], dataOfTimestamp[dataOfTimestamp.length - 1]};
  }

  @Override
  public long[] getDataTime() throws IOException {
    hasRead = true;
    return dataOfTimestamp;
  }
}
