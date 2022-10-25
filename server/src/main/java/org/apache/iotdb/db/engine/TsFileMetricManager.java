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

package org.apache.iotdb.db.engine;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class collect the number and size of tsfile, and send it to the {@link
 * org.apache.iotdb.db.service.metrics.predefined.FileMetrics}
 */
public class TsFileMetricManager {
  private static final TsFileMetricManager INSTANCE = new TsFileMetricManager();
  private AtomicLong seqFileSize = new AtomicLong(0);
  private AtomicLong unseqFileSize = new AtomicLong(0);
  private AtomicInteger seqFileNum = new AtomicInteger(0);
  private AtomicInteger unseqFileNum = new AtomicInteger(0);

  private TsFileMetricManager() {}

  public static TsFileMetricManager getInstance() {
    return INSTANCE;
  }

  public void addFile(long size, boolean seq) {
    if (seq) {
      seqFileSize.getAndAdd(size);
      seqFileNum.incrementAndGet();
    } else {
      unseqFileSize.getAndAdd(size);
      unseqFileNum.incrementAndGet();
    }
  }

  public void deleteFile(long size, boolean seq) {
    if (seq) {
      seqFileSize.getAndAdd(-size);
      seqFileNum.getAndAdd(-1);
    } else {
      unseqFileSize.getAndAdd(-size);
      unseqFileNum.getAndAdd(-1);
    }
  }

  public long getFileSize(boolean seq) {
    return seq ? seqFileSize.get() : unseqFileSize.get();
  }

  public long getFileNum(boolean seq) {
    return seq ? seqFileNum.get() : unseqFileNum.get();
  }
}
