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
 *
 */
package org.apache.iotdb.db.newsync.sender.conf;

import org.apache.iotdb.db.newsync.sender.pipe.Pipe;

import java.io.File;

public class SenderConf {
  public static final String defaultPipeSinkIP = "127.0.0.1";
  public static final int defaultPipeSinkPort = 6670;

  public static final String syncDir = "data" + File.separator + "sync";

  public static final String pipeDir = syncDir + File.separator + "sender pipe ";
  public static final String pipeCollectFinishLockName = "finishCollect.lock";
  public static final String tsFileDirName = "TsFile data";
  public static final String pipeLogDirName = "Pipe log";
  public static final String historyPipeLogName = "pipe data.log.history";
  public static final String realTimePipeLogNameSuffix = "-pipe data.log";
  public static final Long defaultPipeLogSizeInByte = 10485760L;
  public static final String removeSerialNumberLogName = "remove serial number.log";

  public static final String sysDir = syncDir + File.separator + "sys";
  public static final String senderLog = sysDir + File.separator + "senderService.log";
  public static final String planSplitCharacter = ",";
  public static final String senderLogSplitCharacter = " ";

  public static String getPipeDir(Pipe pipe) {
    return pipeDir + pipe.getName() + " " + pipe.getCreateTime();
  }

  public static String getRealTimePipeLogName(long serialNumber) {
    return serialNumber + realTimePipeLogNameSuffix;
  }

  public static Long getSerialNumberFromPipeLogName(String pipeLogName) {
    return Long.parseLong(pipeLogName.split("-")[0]);
  }
}
