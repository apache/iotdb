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
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.SchemaRegionConstant;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.request.IFlushRequest;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FlushManager<T, R extends IFlushRequest>
    extends BasicLSMManager<T, R, FlushRequestContext> {

  // use wal manager object to write wal file on deletion
  private WALManager walManager;

  private IMemManager memManager;

  private ScheduledExecutorService checkFlushThread;

  private String flushDirPath;

  private String flushFilePrefix;

  private final int flushIntervalMs = 1000;

  public FlushManager(
      WALManager walManager, T memManager, String flushDirPath, String flushFilePrefix) {
    this.walManager = walManager;
    if (!(memManager instanceof IMemManager)) {
      throw new ClassCastException("memManager must implement IMemManager");
    }
    this.memManager = (IMemManager) memManager;
    this.flushDirPath = flushDirPath;
    File flushDir = new File(flushDirPath);
    flushDir.mkdirs();
    this.flushFilePrefix = flushFilePrefix;
    checkFlushThread = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("LSM-Flush-Service");
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        checkFlushThread,
        this::checkFlush,
        flushIntervalMs,
        flushIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public void checkFlush() {
    synchronized (memManager) {
      if (memManager.isNeedFlush()) {
        List<R> flushRequests = memManager.getFlushRequests();
        for (R flushRequest : flushRequests) {
          flushRequest.setFlushDirPath(flushDirPath);
          flushRequest.setFlushFileName(flushFilePrefix + "-0-" + flushRequest.getIndex());
          flushRequest.setFlushDeleteFileName(
              flushFilePrefix + "-delete" + "-0-" + flushRequest.getIndex());
          flush(flushRequest);
          memManager.removeMemData(flushRequest);
          updateWal(flushRequest);
        }
      }
    }
  }

  private void updateWal(R request) {
    int index = request.getIndex();
    walManager.deleteWalFile(index);
  }

  private void flush(R flushRequest) {
    FlushRequestContext flushRequestBaseContext = new FlushRequestContext();
    process((T) flushRequest.getMemNode(), flushRequest, flushRequestBaseContext);
  }

  @Override
  public void preProcess(T root, R request, FlushRequestContext context) {
    String flushFileName = request.getFlushFileName() + SchemaRegionConstant.TMP;
    File flushFile = new File(this.flushDirPath, flushFileName);
    try {
      if (!flushFile.exists()) {
        flushFile.createNewFile();
      }
      context.setFileOutput(new FileOutput(flushFile));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void postProcess(T root, R request, FlushRequestContext context) {
    FileOutput fileOutput = context.getFileOutput();
    try {
      fileOutput.close();
      String flushFileName = request.getFlushFileName() + SchemaRegionConstant.TMP;
      File flushFile = new File(this.flushDirPath, flushFileName);
      File newFlushFile = new File(this.flushDirPath, request.getFlushFileName());
      flushFile.renameTo(newFlushFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
