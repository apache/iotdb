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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.commons.concurrent.ThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveLoadMetricsCollector extends ActiveLoadScheduledExecutorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadMetricsCollector.class);

  private final ActiveLoadTsFileLoader activeLoadTsFileLoader;
  private final ActiveLoadDirScanner activeLoadDirScanner;

  private long skipCountPendingFile = 0;
  private long skipCountFailedFile = 0;

  public ActiveLoadMetricsCollector(
      final ActiveLoadTsFileLoader activeLoadTsFileLoader,
      final ActiveLoadDirScanner activeLoadDirScanner) {
    super(ThreadName.ACTIVE_LOAD_METRICS_COLLECTOR);

    this.activeLoadTsFileLoader = activeLoadTsFileLoader;
    this.activeLoadDirScanner = activeLoadDirScanner;

    register(this::countPendingFile);
    register(this::countFailedFile);
    LOGGER.info("Active load metric collector periodical job registered");
  }

  private void countPendingFile() {
    if (skipCountPendingFile == 0) {
      final long currentPendingFileNum =
          activeLoadDirScanner.countAndReportActiveListeningDirsFileNumber();
      // skip skipCountPendingFile * 5 second
      // for example 10000 file will skip 150 second, 100000 will skip 1500 second
      skipCountPendingFile = currentPendingFileNum / 1000 * 3;
    } else {
      --skipCountPendingFile;
    }
  }

  private void countFailedFile() {
    if (skipCountFailedFile == 0) {
      final long currentFailedFileNum = activeLoadTsFileLoader.countAndReportFailedFileNumber();
      // skip skipCountFailedFile * 5 second
      // for example 10000 file will skip 150 second, 100000 will skip 1500 second
      skipCountFailedFile = currentFailedFileNum / 1000 * 3;
    } else {
      --skipCountFailedFile;
    }
  }
}
