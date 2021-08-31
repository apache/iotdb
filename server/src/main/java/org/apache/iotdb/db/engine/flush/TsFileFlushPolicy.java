/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;

/**
 * TsFileFlushPolicy is applied when a TsFileProcessor is full after insertion. For standalone
 * IoTDB, the flush or close is executed without constraint. But in the distributed version, the
 * close is controlled by the leader and should not be performed by the follower alone.
 */
public interface TsFileFlushPolicy {

  void apply(StorageGroupProcessor storageGroupProcessor, TsFileProcessor processor, boolean isSeq);

  class DirectFlushPolicy implements TsFileFlushPolicy {

    @Override
    public void apply(
        StorageGroupProcessor storageGroupProcessor,
        TsFileProcessor tsFileProcessor,
        boolean isSeq) {
      if (tsFileProcessor.shouldClose()) {
        storageGroupProcessor.asyncCloseOneTsFileProcessor(isSeq, tsFileProcessor);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
  }
}
