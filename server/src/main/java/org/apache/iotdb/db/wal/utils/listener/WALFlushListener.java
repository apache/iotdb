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
package org.apache.iotdb.db.wal.utils.listener;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.wal.utils.WALMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class helps judge whether wal is flushed to the storage device. */
public class WALFlushListener implements IResultListener {
  private static final Logger logger = LoggerFactory.getLogger(WALFlushListener.class);
  private static final WALMode mode = IoTDBDescriptor.getInstance().getConfig().getWalMode();

  private volatile Status status;
  private volatile Exception cause;

  public WALFlushListener() {
    status = Status.RUNNING;
    cause = null;
  }

  public synchronized WALFlushListener succeed() {
    status = Status.SUCCESS;
    if (mode == WALMode.SYNC) {
      this.notifyAll();
    }
    return this;
  }

  public synchronized WALFlushListener fail(Exception e) {
    status = Status.FAILURE;
    cause = e;
    if (mode == WALMode.SYNC) {
      this.notifyAll();
    }
    return this;
  }

  public synchronized Status getResult() {
    // block only when wal mode is sync
    if (mode == WALMode.SYNC) {
      while (status == Status.RUNNING) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Waiting for current buffer being flushed interrupted");
        }
      }
    }
    return status;
  }

  public Exception getCause() {
    return cause;
  }
}
