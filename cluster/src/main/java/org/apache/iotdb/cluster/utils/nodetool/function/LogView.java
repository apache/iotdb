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
package org.apache.iotdb.cluster.utils.nodetool.function;

import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.utils.nodetool.Printer;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.File;
import java.util.List;

@Command(name = "log", description = "Print raft logs from a log file")
public class LogView implements Runnable {

  @Option(
      title = "detailed information",
      name = {"-d", "--detail"},
      description = "Show detail information of logs")
  private boolean detail = false;

  @Option(
      title = "path",
      required = true,
      name = {"-path", "--path"},
      description = "Specify a path for accurate hosts information")
  private String path = null;

  @Override
  public void run() {
    SyncLogDequeSerializer logDequeSerializer = new SyncLogDequeSerializer(path);

    List<Log> logs = logDequeSerializer.getAllEntriesAfterAppliedIndex();
    HardState state = logDequeSerializer.getHardState();
    LogManagerMeta managerMeta = logDequeSerializer.getMeta();

    Printer.msgPrintln("-------------------LOG META-------------------------");
    Printer.msgPrintln(managerMeta.toString());
    Printer.msgPrintln("-------------------LOG DATA-------------------------");
    Printer.msgPrintln("-------------------NODE STATE-------------------------");
    Printer.msgPrintln(state.toString());
    Printer.msgPrintln("-------------------NODE STATE-------------------------");

    Printer.msgPrintln("-------------------LOG DATA FILES-------------------------");
    List<File> dataFileList = logDequeSerializer.getLogDataFileList();
    List<File> indexFileList = logDequeSerializer.getLogIndexFileList();
    for (int i = 0; i < dataFileList.size(); i++) {
      Printer.msgPrintln(
          "name=" + dataFileList.get(i).getName() + ",length=" + dataFileList.get(i).length());
      Printer.msgPrintln(
          "name=" + indexFileList.get(i).getName() + ",length=" + indexFileList.get(i).length());
    }

    Printer.msgPrintln("-------------------LOG DATA FILES-------------------------");

    int count = 0;

    for (Log log : logs) {
      Printer.msgPrintln("Log NO " + count + ": ");
      count++;
      if (detail) {
        Printer.msgPrintln(log.toString());
      } else {
        Printer.msgPrintln(log.getClass().getSimpleName());
      }
    }
  }
}
