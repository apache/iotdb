package org.apache.iotdb.cluster.utils.nodetool.function;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.Deque;
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
    Deque<Log> logs = logDequeSerializer.recoverLog();

    Printer.msgPrintln("-------------------LOG META-------------------------");
    Printer.msgPrintln(managerMeta.toString());
    Printer.msgPrintln("-------------------LOG DATA-------------------------");
    int count = 0;

    while (!logs.isEmpty()) {
      Printer.msgPrintln("Log NO " + count + ": ");
      count++;
      Log curLog = logs.removeFirst();
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
