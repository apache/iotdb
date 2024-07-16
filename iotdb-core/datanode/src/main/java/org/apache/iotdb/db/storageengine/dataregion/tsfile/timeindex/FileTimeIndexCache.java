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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex;

import java.util.concurrent.TimeUnit;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

public class FileTimeIndexCache {

  private ScheduledExecutorService recordFileIndexThread;

  private final BlockingQueue<TsFileResource> resourceQueue = new ArrayBlockingQueue<>(100);

  public void add(TsFileResource resource) {
    resourceQueue.add(resource);
  }

//  public void recordTsFileResource() {
//    recordFileIndexThread =
//        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
//            ThreadName.FILE_TIMEINDEX_RECORD.getName());
//        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
//            recordFileIndexThread, this::deleteOutdatedFiles, initDelayMs, periodMs,
//     TimeUnit.MILLISECONDS);
//  }
}
