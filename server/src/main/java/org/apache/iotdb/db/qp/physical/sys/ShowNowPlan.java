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
package org.apache.iotdb.db.qp.physical.sys;

import com.sun.management.OperatingSystemMXBean;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class ShowNowPlan extends ShowPlan {

  public ShowNowPlan() {
    super(ShowContentType.NOW);
  }

  public ShowNowPlan(ShowContentType showContentType) {
    super(showContentType);
  }

  public List<String> now() {
    ArrayList<String> list = new ArrayList<>();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    list.add(df.format(System.currentTimeMillis())); // 添加时间
    final SystemInfo systemInfo = new SystemInfo();
    OperatingSystemMXBean osmxb =
        (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    CentralProcessor processor = systemInfo.getHardware().getProcessor();
    list.add(processor.getSystemCpuLoad() * 100 + ""); // cpu load
    String totalMemorySize =
        new DecimalFormat("#.##").format(osmxb.getTotalPhysicalMemorySize() / 1024.0 / 1024 / 1024)
            + "G";
    String freePhysicalMemorySize =
        new DecimalFormat("#.##").format(osmxb.getFreePhysicalMemorySize() / 1024.0 / 1024 / 1024)
            + "G";
    return list;
  }
}
