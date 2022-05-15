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
package org.apache.iotdb.confignode.manager.sync;

import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.service.transport.thrift.IdentityInfo;

import java.io.File;
import java.io.IOException;

/** Util for path generation in sync module */
public class SyncReceiverPathUtil {

  private SyncReceiverPathUtil() {
    // forbidding instantiation
  }

  // sync data structure
  // data/sync
  // |----sender dir
  // |      |----sender pipe dir
  // |             |----history pipe log dir
  // |             |----realtime pipe log dir
  // |             |----file data dir
  // |----receiver dir
  //        |-----receiver pipe dir
  //                |----receiver pipe log dir
  //                |----file data dir

  /** receiver */
  public static String getReceiverDir() {
    return ConfigNodeDescriptor.getInstance().getConf().getSyncDir()
        + File.separator
        + SyncConstant.RECEIVER_DIR_NAME;
  }

  public static String getReceiverPipeDir(String pipeName, String remoteIp, long createTime) {
    return getReceiverDir()
        + File.separator
        + getReceiverPipeDirName(pipeName, remoteIp, createTime);
  }

  public static String getReceiverPipeDirName(String pipeName, String remoteIp, long createTime) {
    return String.format("%s-%d-%s", pipeName, createTime, remoteIp);
  }

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

  public static String getFileDataDirPath(IdentityInfo identityInfo) {
    return SyncReceiverPathUtil.getReceiverFileDataDir(
        identityInfo.getPipeName(), identityInfo.getAddress(), identityInfo.getCreateTime());
  }

  public static String getPipeLogDirPath(IdentityInfo identityInfo) {
    return SyncReceiverPathUtil.getReceiverPipeLogDir(
        identityInfo.getPipeName(), identityInfo.getAddress(), identityInfo.getCreateTime());
  }

  /** common */
  public static String getSysDir() {
    return ConfigNodeDescriptor.getInstance().getConf().getSyncDir()
        + File.separator
        + SyncConstant.SYNC_SYS_DIR;
  }

  public static String getPipeLogName(long serialNumber) {
    return serialNumber + SyncConstant.PIPE_LOG_NAME_SUFFIX;
  }

  public static Long getSerialNumberFromPipeLogName(String pipeLogName) {
    return Long.parseLong(pipeLogName.split(SyncConstant.PIPE_LOG_NAME_SEPARATOR)[0]);
  }

  public static boolean createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    return file.createNewFile();
  }

  public static String createMsg(String msg) {
    return String.format(
        "[%s] %s", DatetimeUtils.convertLongToDate(DatetimeUtils.currentTime()), msg);
  }
}
