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
