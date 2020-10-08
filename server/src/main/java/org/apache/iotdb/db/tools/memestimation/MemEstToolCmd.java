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
package org.apache.iotdb.db.tools.memestimation;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.service.IoTDB;

@Command(name = "calmem", description = "calculate minimum memory required for writing based on the number of storage groups and timeseries")
public class MemEstToolCmd extends WrappedRunnable {

  @Option(title = "storage group number", name = {"-sg",
      "--storagegroup"}, description = "Storage group number")
  private String sgNumString = "10";

  @Option(title = "total timeseries number", name = {"-ts",
      "--timeseries"}, description = "Total timeseries number")
  private String tsNumString = "1000";

  @Option(title = "max timeseries", name = {"-mts",
      "--maxtimeseries"}, description = "Maximum timeseries number among storage groups, make sure that it's smaller than total timeseries number")
  private String maxTsNumString = "0";

  @Override
  public void runMayThrow() {
    // backup origin config parameters
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long memTableSize = config.getMemtableSizeThreshold();
    int maxMemtableNumber = config.getMaxMemtableNumber();
    long tsFileSize = config.getTsFileSizeThreshold();

    // parse input parameters
    long sgNum = Long.parseLong(sgNumString);
    long tsNum = Long.parseLong(tsNumString);
    long maxTsNum = Long.parseLong(maxTsNumString);

    // tool parameters
    long stepMemory = calStepMemory(tsNum) * IoTDBConstant.GB;
    long currentMemory = stepMemory;
    long maxTsNumValid = maxTsNum;
    long maxProcess = 0;
    long start = System.currentTimeMillis();
    while (true) {

      // recover config parameter
      config.setAllocateMemoryForWrite(currentMemory);
      config.setMemtableSizeThreshold(memTableSize);
      config.setMaxMemtableNumber(maxMemtableNumber);
      config.setTsFileSizeThreshold(tsFileSize);
      IoTDBConfigDynamicAdapter.getInstance().reset();
      IoTDBConfigDynamicAdapter.getInstance().setInitialized(true);
      IoTDB.metaManager.clear();

      long sgCnt = 1;
      long tsCnt = 0;
      try {
        for (; sgCnt <= sgNum; sgCnt++) {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
        }
        for (; tsCnt < tsNum; tsCnt++) {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
          if (maxTsNum == 0) {
            maxTsNumValid = tsCnt / sgNum + 1;
          } else {
            maxTsNumValid = Math.min(tsCnt, maxTsNum);
            maxTsNumValid = Math.max(maxTsNumValid, tsCnt / sgNum + 1);
          }
          IoTDB.metaManager.setMaxSeriesNumberAmongStorageGroup(maxTsNumValid);
        }

      } catch (ConfigAdjusterException e) {
        if (sgCnt > sgNum) {
          maxProcess = Math.max(maxProcess, (tsCnt + 1) * 100 / tsNum);
          System.out
              .print(String.format("Memory estimation progress : %d%%\r", maxProcess));
        }
        currentMemory += stepMemory;
        continue;
      }
      break;
    }
    System.out.println(String
        .format("Memory for writing: %dGB, SG: %d, TS: %d, MTS: %d", currentMemory / IoTDBConstant.GB,
            sgNum, tsNum, maxTsNumValid));
    System.out.println(String.format("Calculating memory for writing consumes: %dms",
        (System.currentTimeMillis() - start)));
  }

  private long calStepMemory(long maxTimeseriesNumber) {
    maxTimeseriesNumber /= 10000000;
    int step = 1;
    while (maxTimeseriesNumber > 0) {
      maxTimeseriesNumber /= 10;
      step *= 10;
    }
    return step;
  }
}
