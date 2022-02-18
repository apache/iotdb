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
package org.apache.iotdb.db.qp.utils;

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
import java.util.LinkedList;
import java.util.List;

public class ShowNowUtils {
  private static final Logger logger = LoggerFactory.getLogger(ShowNowUtils.class);

  private String ipAddress;
  private String systemTime;
  private String cpuLoad;
  private String totalMemorySize;
  private String freeMemorySize;

  public List<ShowNowResult> getShowNowResults() {
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
      e.printStackTrace();
    }
    List<ShowNowResult> showNowResults = new LinkedList<>();
    showNowResults.add(
        new ShowNowResult(ipAddress, systemTime, cpuLoad, totalMemorySize, freeMemorySize));
    return showNowResults;
  }
}
