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
package org.apache.iotdb.db.sync.sender.service;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.recovery.SenderLogger;
import org.apache.iotdb.service.transport.thrift.ResponseType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;

public class MsgManager {
  private static final Logger logger = LoggerFactory.getLogger(MsgManager.class);

  private SenderLogger senderLogger;

  private Pipe runningPipe;
  private Queue<String> Messages;

  public MsgManager() {}

  public MsgManager(SenderLogger senderLogger) {
    this.senderLogger = senderLogger;
  }

  public void addPipe(Pipe pipe) {
    this.runningPipe = pipe;
    Messages = new ArrayDeque<>();
  }

  public void removeAllPipe() {
    runningPipe = null;
    Messages = null;
  }

  public synchronized void recordMsg(
      Pipe pipe, Operator.OperatorType operatorType, ResponseType type, String inputMsg) {
    if (runningPipe == null) {
      logger.warn(
          String.format("No running Pipe for recording msg [%s] %s.", type.name(), inputMsg));
      return;
    } else if (!pipe.equals(runningPipe)) {
      logger.warn(
          String.format(
              "Input Pipe %s is not equal running Pipe %s, ignore it.",
              pipe.getName(), runningPipe.getName()));
    }

    String msg = String.format("[%s] ", type.name()) + SyncPathUtil.createMsg(inputMsg);
    if (Messages.size() > SyncConstant.MESSAGE_NUMBER_LIMIT) {
      Messages.poll();
    }
    Messages.offer(msg);
    if (senderLogger != null) { // not in recover
      senderLogger.recordMsg(pipe.getName(), operatorType, msg);
    }
  }

  public synchronized String getPipeMsg(Pipe pipe) {
    if (runningPipe == null) {
      return "";
    } else if (!pipe.equals(runningPipe)) {
      return "";
    }

    StringBuilder builder = new StringBuilder();
    int size = Messages.size();
    for (int i = 0; i < size; i++) {
      String msg = Messages.poll();
      if (i < SyncConstant.MESSAGE_NUMBER_LIMIT) {
        builder.append(msg);
        //        builder.append(System.lineSeparator()); do not support multi lines now
      }
      Messages.offer(msg);
    }
    if (size > SyncConstant.MESSAGE_NUMBER_LIMIT) {
      builder.append(" ...");
    }
    //    builder.append("  (for More info, check $IOTDB_HOME$/log/ please.)");
    return builder.toString();
  }

  public void recoverMsg(String[] parseStrings) {
    if (parseStrings.length == 3) {
      if (Messages.size() > SyncConstant.MESSAGE_NUMBER_LIMIT) {
        Messages.poll();
      }
      Messages.offer(parseStrings[2]);
    }
  }
}
