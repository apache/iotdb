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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import java.io.IOException;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private final long maxSizeInBytes;

  PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    super(maxDelayInMs);
    this.maxSizeInBytes = requestMaxBatchSizeInBytes;
  }

  @Override
  protected void constructBatch(final TabletInsertionEvent event)
      throws WALPipeException, IOException {}

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
  }

  @Override
  protected long getTotalSize() {
    return 0;
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return maxSizeInBytes;
  }
}
