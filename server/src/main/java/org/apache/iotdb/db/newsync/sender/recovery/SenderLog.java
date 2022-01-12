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
package org.apache.iotdb.db.newsync.sender.recovery;

import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SenderLog {
  private BufferedWriter bw;

  public SenderLog() throws IOException {
    File senderLog = new File(SenderConf.senderLog);
    if (!senderLog.exists()) {
      senderLog.createNewFile();
    }

    bw = new BufferedWriter(new FileWriter(senderLog));
  }

  public void addPipeSink(CreatePipeSinkPlan plan) throws IOException {
    bw.write(Operator.OperatorType.CREATE_PIPESINK.name());
    bw.newLine();
    bw.write(plan.toString());
    bw.newLine();
    bw.flush();
  }

  public void dropPipeSink(String pipeSinkName) throws IOException {
    bw.write(Operator.OperatorType.DROP_PIPESINK.name());
    bw.write(SenderConf.senderLogSplitCharacter);
    bw.write(pipeSinkName);
    bw.newLine();
    bw.flush();
  }

  public void addPipe(CreatePipePlan plan) throws IOException {
    bw.write(Operator.OperatorType.CREATE_PIPE.name());
    bw.newLine();
    bw.write(plan.toString());
    bw.newLine();
    bw.flush();
  }

  public void operatePipe(String pipeName, Operator.OperatorType type) throws IOException {
    bw.write(type.name());
    bw.write(SenderConf.senderLogSplitCharacter);
    bw.write(pipeName);
    bw.newLine();
    bw.flush();
  }

  public void close() throws IOException {
    bw.close();
  }
}
