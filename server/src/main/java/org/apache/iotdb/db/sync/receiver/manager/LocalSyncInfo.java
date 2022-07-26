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
package org.apache.iotdb.db.sync.receiver.manager;

import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.sync.receiver.AbstractSyncInfo;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class LocalSyncInfo extends AbstractSyncInfo {

  public void setCollector() {}

  @Override
  protected void afterStartPipe(String pipeName, String remoteIp, long createTime) {}

  @Override
  protected void afterStopPipe(String pipeName, String remoteIp, long createTime) {}

  @Override
  protected void afterDropPipe(String pipeName, String remoteIp, long createTime) {

    File dir = new File(SyncPathUtil.getReceiverPipeDir(pipeName, remoteIp, createTime));
    try {
      FileUtils.deleteDirectory(dir);
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
  }
}
