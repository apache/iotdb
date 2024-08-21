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

package org.apache.iotdb.db.queryengine.execution.load.active;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveLoadListeningFileCounter extends ActiveLoadManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ActiveLoadListeningFileCounter.class);

  private long skipCountPendingFile = 0;
  private long skipCountFailedFile = 0;

  public void start() {
    register(this::countPendingFile);
    register(this::countFailedFile);
    super.start();
    LOGGER.info("Registering active load metric periodical job");
  }

  private void countPendingFile() {
    if (skipCountPendingFile == 0) {
      final long currentPendingFileNum = ActiveLoadAgent.scanner().countActiveListeningDirsFile();
      // skip skipCountPendingFile * 5 second
      // for example 10000 file will skip 150 second, 100000 will skip 1500 second
      skipCountPendingFile = currentPendingFileNum / 1000 * 3;
    } else {
      --skipCountPendingFile;
    }
  }

  private void countFailedFile() {
    if (skipCountFailedFile == 0) {
      final long currentFailedFileNum = ActiveLoadAgent.loader().countFailedFile();
      // skip skipCountFailedFile * 5 second
      // for example 10000 file will skip 150 second, 100000 will skip 1500 second
      skipCountFailedFile = currentFailedFileNum / 1000 * 3;
    } else {
      --skipCountFailedFile;
    }
  }
}
