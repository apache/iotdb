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
package org.apache.iotdb.db.conf.adjuster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBConfigDynamicAdjusterTest {
  private static IoTDB daemon;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void addOrDeleteStorageGroup() {
    System.out.println(
        "System total memory : " + Runtime.getRuntime().maxMemory() / IoTDBConstant.MB
            + "MB");
    IoTDBConfigDynamicAdjuster.getInstance();
    MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(10000);
    try {
      IoTDBConfigDynamicAdjuster.getInstance().addOrDeleteTimeSeries(10000);
    } catch (ConfigAdjusterException e) {
      fail();
    }
    System.out.println("MemTable size floor threshold is :"
        + MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups()
        * PrimitiveArrayPool.ARRAY_SIZE * Long.BYTES * 2 / IoTDBConstant.MB + "MB");
    for (int i = 1; i < 100; i++) {
      try {
        IoTDBConfigDynamicAdjuster.getInstance().addOrDeleteStorageGroup(1);
        assertEquals(IoTDBConfigDynamicAdjuster.getInstance().getCurrentMemTableSize(),
            TSFileConfig.groupSizeInByte);
        System.out.println(String.format("add %d storage groups, the memTableSize is %dMB, the tsFileSize is %dMB", i,
            TSFileConfig.groupSizeInByte / IoTDBConstant.MB,
            IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold() / IoTDBConstant.MB));
      } catch (ConfigAdjusterException e) {
        fail();
      }
    }
  }

  @Test
  public void addOrDeleteTimeSeries() {
    System.out.println(
        "System total memory : " + Runtime.getRuntime().maxMemory() / IoTDBConstant.MB
            + "MB");
    try {
      IoTDBConfigDynamicAdjuster.getInstance().addOrDeleteStorageGroup(1);
      for(int i = 1; i <= 100000 ; i++) {
        if(i ==27780){
          System.out.println(i);
        }
        System.out.println("MemTable size floor threshold is :"
            + MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups()
            * PrimitiveArrayPool.ARRAY_SIZE * Long.BYTES * 2/ IoTDBConstant.MB + "MB");
        IoTDBConfigDynamicAdjuster.getInstance().addOrDeleteTimeSeries(1);
        MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(i);
        assertEquals(IoTDBConfigDynamicAdjuster.getInstance().getCurrentMemTableSize(),
            TSFileConfig.groupSizeInByte);
        System.out.println(String
            .format("add %d timeseries, the memTableSize is %dMB, the tsFileSize is %dMB", i,
                TSFileConfig.groupSizeInByte / IoTDBConstant.MB,
                IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold()
                    / IoTDBConstant.MB));
      }
    } catch (ConfigAdjusterException e) {
      fail();
    }
  }

  @Test
  public void addOrDeleteTimeSeriesSyso() throws IOException {
    int sgNum = 1;
    String fileName = "/Users/litianan/Desktop/" + sgNum + "sg.csv";
    FileWriter fw = new FileWriter(new File(fileName));
    fw.write("timeseries,memtable size(MB), memtable threshold(MB), tsfile size(MB)\n");
    System.out.println(
        "System total memory : " + Runtime.getRuntime().maxMemory() / IoTDBConstant.MB
            + "MB");
    try {
      for(int i = 1; i <= 100000 ; i++) {
        if(i % 100 == 0){
          IoTDBConfigDynamicAdjuster.getInstance().addOrDeleteStorageGroup(sgNum);
        }
        fw.write(String.format("%d,%d,%d,%d\n", i, TSFileConfig.groupSizeInByte / IoTDBConstant.MB, MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups()
            * PrimitiveArrayPool.ARRAY_SIZE * Long.BYTES * 2/ IoTDBConstant.MB, IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold()
            / IoTDBConstant.MB));
        System.out.println("MemTable size floor threshold is :"
            + MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups()
            * PrimitiveArrayPool.ARRAY_SIZE * Long.BYTES * 2/ IoTDBConstant.MB + "MB");
        IoTDBConfigDynamicAdjuster.getInstance().addOrDeleteTimeSeries(1);
        MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(i);
        assertEquals(IoTDBConfigDynamicAdjuster.getInstance().getCurrentMemTableSize(),
            TSFileConfig.groupSizeInByte);
        System.out.println(String
            .format("add %d timeseries, the memTableSize is %dMB, the tsFileSize is %dMB", i,
                TSFileConfig.groupSizeInByte / IoTDBConstant.MB,
                IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold()
                    / IoTDBConstant.MB));
      }
    } catch (ConfigAdjusterException e) {
//      fail();
    }
    fw.flush();
    fw.close();
  }
}