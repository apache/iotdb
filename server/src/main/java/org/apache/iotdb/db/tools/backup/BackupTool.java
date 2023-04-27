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

package org.apache.iotdb.db.tools.backup;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.service.rpc.thrift.TSBackupReq;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupTool {
  private static final Logger logger = LoggerFactory.getLogger(BackupTool.class);
  private static final IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
  private static TTransport transport;
  private static TSIService.Client client;
  private static String outputCanonicalPathStr;
  private static boolean isFullBackup = true;

  private static String FULL_BACKUP_PARAM = "-o";
  private static String INCREMENTAL_BACKUP_PARAM = "-b";

  public static void main(String[] args) {
    Thread.currentThread().setName(ThreadName.BACKUP_CLIENT.getName());
    if (!checkArgs(args)) return;
    RpcTransportFactory.setDefaultBufferCapacity(ioTDBConfig.getThriftDefaultBufferSize());
    RpcTransportFactory.setThriftMaxFrameSize(ioTDBConfig.getThriftMaxFrameSize());
    try {
      transport = RpcTransportFactory.INSTANCE.getTransport("127.0.0.1", 6667, 2000);
      transport.open();
    } catch (TTransportException e) {
      logger.error("Cannot connect to the receiver.");
    }
    if (ioTDBConfig.isRpcThriftCompressionEnable()) {
      client = new TSIService.Client(new TCompactProtocol(transport));
    } else {
      client = new TSIService.Client(new TBinaryProtocol(transport));
    }
    try {
      TSStatus status = client.executeBackup(new TSBackupReq(outputCanonicalPathStr, isFullBackup));
      System.out.println(status.code);
    } catch (TException e) {
      e.printStackTrace();
    }
    transport.close();
  }

  private static boolean checkArgs(String[] args) {
    if (args.length != 2) {
      logger.error("Invalid argument count for backup, should be 2 arguments.");
      return false;
    }
    try {
      outputCanonicalPathStr = new File(args[1]).getCanonicalPath();
    } catch (IOException e) {
      logger.error("Invalid backup path.");
      return false;
    }
    if (args[0].equals(FULL_BACKUP_PARAM)) {
      isFullBackup = true;
    } else if (args[0].equals(INCREMENTAL_BACKUP_PARAM)) {
      isFullBackup = false;
    } else {
      logger.error("Invalid parameter.");
      return false;
    }
    return true;
  }
}
