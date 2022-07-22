package org.apache.iotdb.it.env;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EnvUtils {
  private static final String lockFilePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "lock-";

  public static int[] searchAvailablePorts() {
    do {
      int randomPortStart = 1000 + (int) (Math.random() * (1999 - 1000));
      randomPortStart = randomPortStart * 10 + 1;
      File lockFile = new File(getLockFilePath(randomPortStart));
      if (lockFile.exists()) {
        continue;
      }

      List<Integer> requiredPorts =
          IntStream.rangeClosed(randomPortStart, randomPortStart + 9)
              .boxed()
              .collect(Collectors.toList());
      try {
        if (checkPortsAvailable(requiredPorts) && lockFile.createNewFile()) {
          return requiredPorts.stream().mapToInt(Integer::intValue).toArray();
        }
      } catch (IOException e) {
        // ignore
      }
    } while (true);
  }

  private static boolean checkPortsAvailable(List<Integer> ports) {
    String cmd = getSearchAvailablePortCmd(ports);
    try {
      Process proc = Runtime.getRuntime().exec(cmd);
      return proc.waitFor() == 1;
    } catch (IOException e) {
      // ignore
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  private static String getSearchAvailablePortCmd(List<Integer> ports) {
    if (SystemUtils.IS_OS_WINDOWS) {
      return getWindowsSearchPortCmd(ports);
    }
    return getUnixSearchPortCmd(ports);
  }

  private static String getWindowsSearchPortCmd(List<Integer> ports) {
    String cmd = "netstat -aon -p tcp | findStr ";
    return cmd
        + ports.stream().map(v -> "/C:'127.0.0.1:" + v + "'").collect(Collectors.joining(" "));
  }

  private static String getUnixSearchPortCmd(List<Integer> ports) {
    String cmd = "lsof -iTCP -sTCP:LISTEN -P -n | awk '{print $9}' | grep -E ";
    return cmd + ports.stream().map(String::valueOf).collect(Collectors.joining("|")) + "\"";
  }

  public static String getLockFilePath(int port) {
    return lockFilePath + port;
  }
}
