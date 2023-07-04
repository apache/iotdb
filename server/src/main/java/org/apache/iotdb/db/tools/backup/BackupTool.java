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

import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.service.rpc.thrift.TSBackupReq;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupTool {
  private static final Logger logger = LoggerFactory.getLogger(BackupTool.class);
  private static TTransport transport;
  private static TSIService.Client client;
  private static String outputCanonicalPathStr = null;
  private static String host = "127.0.0.1";
  private static int port = 6667;
  private static boolean isFullBackup = true;
  private static boolean isSyncBackup = true;

  private static final String HOST_PARAM = "-h";
  private static final String PORT_PARAM = "-p";
  private static final String PATH_PARAM = "-P";
  private static final String FULL_BACKUP_PARAM = "-f";
  private static final String DIFFERENTIAL_BACKUP_PARAM = "-d";
  private static final String SYNC_BACKUP_PARAM = "-sync";
  private static final String ASYNC_BACKUP_PARAM = "-async";

  public static void main(String[] args) {
    if (!checkArgs(args)) return;
    try {
      transport = RpcTransportFactory.INSTANCE.getTransport(host, port, 5000);
      transport.open();
      client = new TSIService.Client(new TBinaryProtocol(transport));
      try {
        logger.info(
            String.format(
                "Performing %s %s backup, host: %s, port: %s, target path is %s.",
                isSyncBackup ? "sync" : "async",
                isFullBackup ? "full" : "differential",
                host,
                port,
                outputCanonicalPathStr));
        TSStatus status =
            client.executeBackup(
                new TSBackupReq(outputCanonicalPathStr, isFullBackup, isSyncBackup));
        logger.info(String.valueOf(status.code));
      } catch (TException e) {
        logger.error("TException happened during backup. Message: " + e.getMessage());
      }
    } catch (TTransportException e) {
      logger.error(
          String.format("Cannot connect to the receiver, host: %s, port: %s.", host, port));
    } finally {
      transport.close();
    }
  }

  private static boolean checkArgs(String[] args) {
    if (args.length < 2) {
      logger.error("Too few arguments for backup, should be at least 2 arguments.");
      return false;
    }
    for (int i = 0; i < args.length; ++i) {
      switch (args[i]) {
        case FULL_BACKUP_PARAM:
          isFullBackup = true;
          break;
        case DIFFERENTIAL_BACKUP_PARAM:
          isFullBackup = false;
          break;
        case SYNC_BACKUP_PARAM:
          isSyncBackup = true;
          break;
        case ASYNC_BACKUP_PARAM:
          isSyncBackup = false;
          break;
        case HOST_PARAM:
          if (i < args.length - 1) {
            host = args[i + 1];
            i++;
          }
          break;
        case PORT_PARAM:
          if (i < args.length - 1) {
            port = Integer.parseInt(args[i + 1]);
            i++;
          }
          break;
        case PATH_PARAM:
          if (i < args.length - 1) {
            try {
              outputCanonicalPathStr = new File(args[i + 1]).getCanonicalPath();
            } catch (IOException e) {
              logger.error("Invalid backup path.");
              return false;
            }
            i++;
          }
          break;
        default:
          logger.error(
              String.format("Invalid backup parameter: %s, please check the format.", args[i]));
          return false;
      }
    }
    if (outputCanonicalPathStr == null) {
      logger.error("Please provide the output path by '-P' parameter.");
      return false;
    }
    return true;
  }
}
