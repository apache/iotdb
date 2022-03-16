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
package org.apache.iotdb.db.newsync.conf;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.File;
import java.io.IOException;

/** Util for path generation in sync module */
public class SyncPathUtil {

  private SyncPathUtil() {
    // forbidding instantiation
  }

  /** sender */
  public static String getSenderPipeDir(String pipeName, long createTime) {
    return IoTDBDescriptor.getInstance().getConfig().getNewSyncDir()
        + File.separator
        + SyncConstant.SENDER_PIPE_DIR_NAME
        + String.format("-%s-%d", pipeName, createTime);
  }

  public static String getSenderHistoryPipeDataDir(String pipeName, long createTime) {
    return getSenderPipeDir(pipeName, createTime)
        + File.separator
        + SyncConstant.HISTORY_PIPE_LOG_DIR_NAME;
  }

  public static String getSenderRealTimePipeDataDir(String pipeName, long createTime) {
    return getSenderPipeDir(pipeName, createTime) + File.separator + SyncConstant.PIPE_LOG_DIR_NAME;
  }

  /** receiver */
  public static String getReceiverPipeLogDir(String pipeName, String remoteIp, long createTime) {
    return getReceiverPipeDir(pipeName, remoteIp, createTime)
        + File.separator
        + SyncConstant.PIPE_LOG_DIR_NAME;
  }

  public static String getReceiverFileDataDir(String pipeName, String remoteIp, long createTime) {
    return getReceiverPipeDir(pipeName, remoteIp, createTime)
        + File.separator
        + SyncConstant.FILE_DATA_DIR_NAME;
  }

  public static String getReceiverPipeDir(String pipeName, String remoteIp, long createTime) {
    return getReceiverDir()
        + File.separator
        + getReceiverPipeFolderName(pipeName, remoteIp, createTime);
  }

  public static String getReceiverPipeFolderName(
      String pipeName, String remoteIp, long createTime) {
    return String.format("%s-%d-%s", pipeName, createTime, remoteIp);
  }

  public static String getReceiverDir() {
    return IoTDBDescriptor.getInstance().getConfig().getNewSyncDir()
        + File.separator
        + SyncConstant.RECEIVER_DIR;
  }

  public static String getSysDir() {
    return IoTDBDescriptor.getInstance().getConfig().getNewSyncDir()
        + File.separator
        + SyncConstant.SYNC_SYS_DIR;
  }

  /** common */
  public static boolean createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    return file.createNewFile();
  }
}
