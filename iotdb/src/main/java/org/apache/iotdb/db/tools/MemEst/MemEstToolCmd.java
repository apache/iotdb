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
package org.apache.iotdb.db.tools.MemEst;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.metadata.MManager;

@Command(name = "calmem", description = "calculate minimum memory required for writing based on the number of storage groups and timeseries")
public class MemEstToolCmd implements Runnable {

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
  public void run() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long memTableSize = config.getMemtableSizeThreshold();
    int maxMemtableNumber = config.getMaxMemtableNumber();
    long tsFileSize = config.getTsFileSizeThreshold();
    long memory = IoTDBConstant.GB;
    long sgNum = Long.parseLong(sgNumString);
    long tsNum = Long.parseLong(tsNumString);
    long maxTsNum = Long.parseLong(maxTsNumString);
    long maxTsNumValid = maxTsNum;
    long maxProcess = 0;

    while (true) {
      // init parameter
      config.setAllocateMemoryForWrite(memory);
      config.setMemtableSizeThreshold(memTableSize);
      config.setMaxMemtableNumber(maxMemtableNumber);
      config.setTsFileSizeThreshold(tsFileSize);
      IoTDBConfigDynamicAdapter.getInstance().reset();
      IoTDBConfigDynamicAdapter.getInstance().setInitialized(true);
      MManager.getInstance().clear();

      long sgCnt = 1;
      long tsCnt = 1;
      try {
        for (; sgCnt <= sgNum; sgCnt++) {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
        }
        for (; tsCnt <= tsNum; tsCnt++) {
          IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
          if(maxTsNum == 0){
            maxTsNumValid = tsCnt / sgNum + 1;
          } else {
            maxTsNumValid = Math.min(tsCnt, maxTsNum);
            maxTsNumValid = Math.max(maxTsNumValid, tsCnt / sgNum + 1);
          }
          MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(maxTsNumValid);
        }

      } catch (ConfigAdjusterException e) {
        if(sgCnt > sgNum) {
          maxProcess = Math.max(maxProcess, tsCnt * 100 / tsNum);
          System.out
              .print(String.format("Memory estimation progress : %d%%\r", maxProcess));
        }
        memory += IoTDBConstant.GB;
        continue;
      }
      break;
    }
    System.out.println(String
        .format("Memory for writing: %dGB, SG: %d, TS: %d, MTS: %d", memory / IoTDBConstant.GB,
            sgNum, tsNum, maxTsNumValid));
  }
}
