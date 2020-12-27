/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MemTableManager {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final Logger logger = LoggerFactory.getLogger(MemTableManager.class);

  private static final int WAIT_TIME = 100;
  public static final int MEMTABLE_NUM_FOR_EACH_PARTITION = 4;
  private int currentMemtableNumber = 0;

  private MemTableManager() {
  }

  public static MemTableManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Called when memory control is disabled
   */
  public synchronized IMemTable getAvailableMemTable(TsFileResource tsFileResource) {
    if (!reachMaxMemtableNumber()) {
      currentMemtableNumber++;
      return new PrimitiveMemTable();
    } else {
      // wait until the total memtable number is less than the capacity of the system
      int waitCount = 1;
      while (true) {
        if (!reachMaxMemtableNumber()) {
          currentMemtableNumber++;
          return new PrimitiveMemTable();
        }
        try {
          TimeUnit.MILLISECONDS.sleep(WAIT_TIME);
        } catch (InterruptedException e) {
          logger.error("{} fails to wait for memtables {}, continue to wait", tsFileResource, e);
          Thread.currentThread().interrupt();
        }
        logger.info("{} has waited for a memtable for {}ms", tsFileResource, waitCount++ * 100);
      }
    }
  }

  public synchronized int getCurrentMemtableNumber() {
    return currentMemtableNumber;
  }

  public synchronized void addMemtableNumber() {
    currentMemtableNumber++;
  }

  public synchronized void decreaseMemtableNumber() {
    currentMemtableNumber--;
  }

  /**
   * Called when memory control is disabled
   */
  private boolean reachMaxMemtableNumber() {
    return currentMemtableNumber >= CONFIG.getMaxMemtableNumber();
  }

  /**
   * Called when memory control is disabled
   */
  public synchronized void addOrDeleteStorageGroup(int diff) {
    int maxMemTableNum = CONFIG.getMaxMemtableNumber();
    maxMemTableNum += MEMTABLE_NUM_FOR_EACH_PARTITION 
        * CONFIG.getConcurrentWritingTimePartition() * diff;
    CONFIG.setMaxMemtableNumber(maxMemTableNum);
  }

  private static class InstanceHolder {

    private static final MemTableManager INSTANCE = new MemTableManager();

    private InstanceHolder() {
    }
  }
}
