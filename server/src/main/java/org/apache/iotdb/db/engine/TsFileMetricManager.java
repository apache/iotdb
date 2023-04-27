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

import org.apache.iotdb.db.service.metrics.FileMetrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** This class collect the number and size of tsfile, and send it to the {@link FileMetrics} */
public class TsFileMetricManager {
  private static final TsFileMetricManager INSTANCE = new TsFileMetricManager();
  private final AtomicLong seqFileSize = new AtomicLong(0);
  private final AtomicLong unseqFileSize = new AtomicLong(0);
  private final AtomicInteger seqFileNum = new AtomicInteger(0);
  private final AtomicInteger unseqFileNum = new AtomicInteger(0);

  private final AtomicInteger modFileNum = new AtomicInteger(0);

  private final AtomicLong modFileSize = new AtomicLong(0);

  // compaction temporal files
  private final AtomicLong innerSeqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong innerUnseqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong crossCompactionTempFileSize = new AtomicLong(0);
  private final AtomicInteger innerSeqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger innerUnseqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger crossCompactionTempFileNum = new AtomicInteger(0);

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

  public void deleteFile(long size, boolean seq, int num) {
    if (seq) {
      seqFileSize.getAndAdd(-size);
      seqFileNum.getAndAdd(-num);
    } else {
      unseqFileSize.getAndAdd(-size);
      unseqFileNum.getAndAdd(-num);
    }
  }

  public long getFileSize(boolean seq) {
    return seq ? seqFileSize.get() : unseqFileSize.get();
  }

  public long getFileNum(boolean seq) {
    return seq ? seqFileNum.get() : unseqFileNum.get();
  }

  public int getModFileNum() {
    return modFileNum.get();
  }

  public long getModFileSize() {
    return modFileSize.get();
  }

  public void increaseModFileNum(int num) {
    modFileNum.addAndGet(num);
  }

  public void decreaseModFileNum(int num) {
    modFileNum.addAndGet(-num);
  }

  public void increaseModFileSize(long size) {
    modFileSize.addAndGet(size);
  }

  public void decreaseModFileSize(long size) {
    modFileSize.addAndGet(-size);
  }

  public void addCompactionTempFileSize(boolean innerSpace, boolean seq, long delta) {
    if (innerSpace) {
      long unused =
          seq
              ? innerSeqCompactionTempFileSize.addAndGet(delta)
              : innerUnseqCompactionTempFileSize.addAndGet(delta);
    } else {
      crossCompactionTempFileSize.addAndGet(delta);
    }
  }

  public void addCompactionTempFileNum(boolean innerSpace, boolean seq, int delta) {
    if (innerSpace) {
      long unused =
          seq
              ? innerSeqCompactionTempFileNum.addAndGet(delta)
              : innerUnseqCompactionTempFileNum.addAndGet(delta);
    } else {
      crossCompactionTempFileNum.addAndGet(delta);
    }
  }

  public long getInnerCompactionTempFileSize(boolean seq) {
    return seq ? innerSeqCompactionTempFileSize.get() : innerUnseqCompactionTempFileSize.get();
  }

  public long getCrossCompactionTempFileSize() {
    return crossCompactionTempFileSize.get();
  }

  public long getInnerCompactionTempFileNum(boolean seq) {
    return seq ? innerSeqCompactionTempFileNum.get() : innerUnseqCompactionTempFileNum.get();
  }

  public long getCrossCompactionTempFileNum() {
    return crossCompactionTempFileNum.get();
  }
}
