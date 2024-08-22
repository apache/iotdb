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

  private long countPendingFileRemainingSkipRound = 0;
  private long countFailedFileRemainingSkipRound = 0;

  public ActiveLoadMetricsCollector(
      final ActiveLoadTsFileLoader activeLoadTsFileLoader,
      final ActiveLoadDirScanner activeLoadDirScanner) {
    super(ThreadName.ACTIVE_LOAD_METRICS_COLLECTOR);

    this.activeLoadTsFileLoader = activeLoadTsFileLoader;
    this.activeLoadDirScanner = activeLoadDirScanner;

    register(this::countAndReportPendingFile);
    register(this::countAndReportFailedFile);
    LOGGER.info("Active load metric collector periodical jobs registered");
  }

  private void countAndReportPendingFile() {
    if (countPendingFileRemainingSkipRound > 0) {
      --countPendingFileRemainingSkipRound;
      return;
    }

    final long currentPendingFileNumber =
        activeLoadDirScanner.countAndReportActiveListeningDirsFileNumber();

    if (currentPendingFileNumber < 100) {
      countPendingFileRemainingSkipRound = 6; // 30 seconds
      return;
    }
    if (currentPendingFileNumber < 1000) {
      countPendingFileRemainingSkipRound = 18; // 90 seconds
      return;
    }
    if (currentPendingFileNumber < 10000) {
      countPendingFileRemainingSkipRound = 120; // 600 seconds
      return;
    }
    countPendingFileRemainingSkipRound = 180; // 900 seconds
  }

  private void countAndReportFailedFile() {
    if (countFailedFileRemainingSkipRound > 0) {
      --countFailedFileRemainingSkipRound;
      return;
    }

    final long currentFailedFileNumber = activeLoadTsFileLoader.countAndReportFailedFileNumber();

    if (currentFailedFileNumber < 100) {
      countFailedFileRemainingSkipRound = 6; // 30 seconds
      return;
    }
    if (currentFailedFileNumber < 1000) {
      countFailedFileRemainingSkipRound = 18; // 90 seconds
      return;
    }
    if (currentFailedFileNumber < 10000) {
      countFailedFileRemainingSkipRound = 120; // 600 seconds
      return;
    }
    countFailedFileRemainingSkipRound = currentFailedFileNumber / 50;
  }
}
