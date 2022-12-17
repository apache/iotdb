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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.request.IFlushRequest;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FlushManager<T extends IMemManager, R extends IFlushRequest>
    extends BasicLSMManager<T, R, FlushRequestContext> {

  // use wal manager object to write wal file on deletion
  private WALManager walManager;

  private T memManager;

  private ScheduledExecutorService checkFlushThread;

  private final int flushIntervalMs = 60_000;

  public FlushManager(WALManager walManager, T memManager) {
    this.walManager = walManager;
    this.memManager = memManager;
    checkFlushThread = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("LSM-Flush-Service");
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        checkFlushThread,
        this::checkFlush,
        flushIntervalMs,
        flushIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public void checkFlush() {
    if (memManager.isNeedFlush()) {
      List<R> flushRequests = memManager.getFlushRequests();
      for (R flushRequest : flushRequests) {
        FlushRequestContext flushRequestContext = flush(flushRequest);
        updateWal(flushRequestContext);
      }
    }
  }

  private void updateWal(FlushRequestContext flushRequestBaseContext) {
    // TODO delete wal file
  }

  private void flushToDisk(FlushRequestContext flushRequestBaseContext) {
    // TODO flush to disk
  }

  private FlushRequestContext flush(R flushRequest) {
    FlushRequestContext flushRequestBaseContext = new FlushRequestContext();
    process(memManager, flushRequest, flushRequestBaseContext);
    flushToDisk(flushRequestBaseContext);
    return flushRequestBaseContext;
  }

  @Override
  public void preProcess(T root, R request, FlushRequestContext context) {}

  @Override
  public void postProcess(T root, R request, FlushRequestContext context) {}
}
