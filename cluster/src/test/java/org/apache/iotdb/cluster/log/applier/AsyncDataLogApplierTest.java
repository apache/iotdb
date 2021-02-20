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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;

public class AsyncDataLogApplierTest {

  private List<Log> logsToApply;
  private Set<Log> appliedLogs;

  @Before
  public void setUp() throws Exception {
    logsToApply = new ArrayList<>();
    appliedLogs = new ConcurrentSkipListSet<>();
    IoTDB.metaManager.init();
    for (int i = 0; i < 10; i++) {
      IoTDB.metaManager.setStorageGroup(new PartialPath(TestUtils.getTestSg(i)));
    }
  }

  @After
  public void tearDown() throws IOException {
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void test() throws IllegalPathException, InterruptedException {
    LogApplier dummyApplier =
        log -> {
          if (log instanceof PhysicalPlanLog) {
            PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
            PhysicalPlan plan = physicalPlanLog.getPlan();
            if (plan instanceof InsertRowPlan) {
              appliedLogs.add(log);
              log.setApplied(true);
            }
          } else {
            // make sure all previous insertions are applied before applying the last log
            if (appliedLogs.size() == 10) {
              appliedLogs.add(log);
              log.setApplied(true);
            }
          }
        };
    AsyncDataLogApplier asyncDataLogApplier = new AsyncDataLogApplier(dummyApplier, "test");
    try {
      for (int i = 0; i < 10; i++) {
        PhysicalPlan plan =
            new InsertRowPlan(
                new PartialPath(TestUtils.getTestSg(i)), i, new String[0], new String[0]);
        PhysicalPlanLog log = new PhysicalPlanLog(plan);
        log.setCurrLogIndex(i);
        logsToApply.add(log);
      }
      Log finalLog = new EmptyContentLog();
      finalLog.setCurrLogIndex(10);
      logsToApply.add(finalLog);

      for (Log log : logsToApply) {
        asyncDataLogApplier.apply(log);
      }

      synchronized (finalLog) {
        while (!finalLog.isApplied()) {
          finalLog.wait();
        }
      }
      assertEquals(11, appliedLogs.size());
    } finally {
      asyncDataLogApplier.close();
    }
  }

  @Test
  public void testParallel() {
    LogApplier dummyApplier =
        log -> {
          if (log instanceof PhysicalPlanLog) {
            PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
            PhysicalPlan plan = physicalPlanLog.getPlan();
            if (plan instanceof InsertRowPlan) {
              appliedLogs.add(log);
              log.setApplied(true);
            }
          } else {
            appliedLogs.add(log);
            log.setApplied(true);
          }
        };

    AsyncDataLogApplier asyncDataLogApplier = new AsyncDataLogApplier(dummyApplier, "test");

    try {
      for (int i = 0; i < 10; i++) {
        int finalI = i;
        new Thread(
                () -> {
                  List<Log> threadLogsToApply = new ArrayList<>();
                  for (int j = 0; j < 10; j++) {
                    PhysicalPlan plan = null;
                    try {
                      plan =
                          new InsertRowPlan(
                              new PartialPath(TestUtils.getTestSg(finalI)),
                              j,
                              new String[0],
                              new String[0]);
                    } catch (IllegalPathException e) {
                      // ignore
                    }
                    PhysicalPlanLog log = new PhysicalPlanLog(plan);
                    log.setCurrLogIndex(finalI * 11 + j);
                    threadLogsToApply.add(log);
                  }
                  Log finalLog = new EmptyContentLog();
                  finalLog.setCurrLogIndex(finalI * 11 + 10);
                  threadLogsToApply.add(finalLog);

                  for (Log log : threadLogsToApply) {
                    asyncDataLogApplier.apply(log);
                  }
                })
            .start();
      }

      while (appliedLogs.size() < 11 * 10) {}

      assertEquals(110, appliedLogs.size());
    } finally {
      asyncDataLogApplier.close();
    }
  }
}
