/**
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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.List;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.utils.nodetool.Printer;

@Command(name = "log", description = "Print raft logs from a log file")
public class LogView implements Runnable {

  @Option(title = "detailed information", name = {"-d",
      "--detail"}, description = "Show detail information of logs")
  private boolean detail = false;

  @Option(title = "path", required = true, name = {"-path",
      "--path"}, description = "Specify a path for accurate hosts information")
  private String path = null;

  @Override
  public void run() {
    SyncLogDequeSerializer logDequeSerializer = new SyncLogDequeSerializer(path);

    LogManagerMeta managerMeta = logDequeSerializer.recoverMeta();
    List<Log> logs = logDequeSerializer.recoverLog();

    Printer.msgPrintln("-------------------LOG META-------------------------");
    Printer.msgPrintln(managerMeta.toString());
    Printer.msgPrintln("-------------------LOG DATA-------------------------");
    int count = 0;

    for (int i = 0; i < logs.size(); i++) {
      Printer.msgPrintln("Log NO " + count + ": ");
      count++;
      Log curLog = logs.get(i);
      if (detail) {
        Printer.msgPrintln(curLog.toString());
      } else {
        Printer.msgPrintln(
            curLog.getClass().getSimpleName() + " Size: " +
                logDequeSerializer.getLogSizeDeque().removeFirst());
      }
    }
  }
}
