package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.query.dataset.ShowNowResult;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ShowNowUtils {
  private static final Logger logger = LoggerFactory.getLogger(ShowNowUtils.class);

  String ipAddress;
  String systemTime;
  String cpuLoad;
  String totalMemorySize;
  String freeMemorySize;

  public List<String> now() {
    ArrayList<String> list = new ArrayList<>();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    SystemInfo systemInfo = new SystemInfo();
    try {
      ipAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      OperatingSystemMXBean osmxb =
          (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      CentralProcessor processor = systemInfo.getHardware().getProcessor();
      totalMemorySize =
          new DecimalFormat("#.##")
                  .format(osmxb.getTotalPhysicalMemorySize() / 1024.0 / 1024 / 1024)
              + "G";
      freeMemorySize =
          new DecimalFormat("#.##").format(osmxb.getFreePhysicalMemorySize() / 1024.0 / 1024 / 1024)
              + "G";
      systemTime = df.format(System.currentTimeMillis());
      cpuLoad = new DecimalFormat("#.##").format(processor.getSystemCpuLoad() * 100) + "%";
    } catch (Exception e) {
      logger.error("***************    抓取到错误");
      e.printStackTrace();
    }

    list.add(ipAddress);
    list.add(systemTime);
    list.add(cpuLoad);
    list.add(totalMemorySize);
    list.add(freeMemorySize);
    return list;
  }

  public List<ShowNowResult> getShowNowResults() {
    List<String> list = now();
    List<ShowNowResult> showNowResults = new LinkedList<>();
    showNowResults.add(
        new ShowNowResult(list.get(0), list.get(1), list.get(2), list.get(3), list.get(4)));
    return showNowResults;
  }
}
