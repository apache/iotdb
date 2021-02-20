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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;

public class FakedBatchReader implements IBatchReader {

  private BatchData data;
  private boolean hasCached = false;

  public FakedBatchReader(long[] timestamps) {
    data = new BatchData(TSDataType.INT32);
    for (long time : timestamps) {
      data.putInt(time, 1);
      hasCached = true;
    }
  }

  @Override
  public boolean hasNextBatch() {
    return hasCached;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (data == null || !data.hasCurrent()) {
      throw new IOException("no next batch");
    }
    hasCached = false;
    return data;
  }

  @Override
  public void close() {}
}
