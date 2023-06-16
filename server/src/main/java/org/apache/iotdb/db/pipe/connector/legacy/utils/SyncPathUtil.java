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

package org.apache.iotdb.db.pipe.connector.legacy.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.legacy.transport.SyncIdentityInfo;

import java.io.File;

/** Util for path generation in sync module */
public class SyncPathUtil {

  private SyncPathUtil() {
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
  // |      |-----receiver pipe dir
  // |              |----receiver pipe log dir
  // |              |----file data dir
  // |----sys dir

  /** receiver */
  public static String getReceiverDir() {
    return CommonDescriptor.getInstance().getConfig().getSyncDir()
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

  public static String getReceiverFileDataDir(String pipeName, String remoteIp, long createTime) {
    return getReceiverPipeDir(pipeName, remoteIp, createTime)
        + File.separator
        + SyncConstant.FILE_DATA_DIR_NAME;
  }

  public static String getFileDataDirPath(SyncIdentityInfo identityInfo) {
    return SyncPathUtil.getReceiverFileDataDir(
        identityInfo.getPipeName(), identityInfo.getRemoteAddress(), identityInfo.getCreateTime());
  }
}
