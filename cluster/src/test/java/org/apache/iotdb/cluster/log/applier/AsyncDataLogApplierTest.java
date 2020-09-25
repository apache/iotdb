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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AsyncDataLogApplierTest {

  private List<Log> logsToApply;
  private Set<Log> appliedLogs;
  private static final String TEST_SG = "root.test";

  @Before
  public void setUp() throws Exception {
    logsToApply = new ArrayList<>();
    appliedLogs = new HashSet<>();
    IoTDB.metaManager.init();
    for (int i = 0; i < 10; i++) {
      IoTDB.metaManager.setStorageGroup(new PartialPath(TEST_SG + i));
    }
  }

  @After
  public void tearDown() throws IOException {
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void test() throws IllegalPathException, InterruptedException, StorageGroupNotSetException {
    LogApplier dummyApplier = log -> {
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
    AsyncDataLogApplier asyncDataLogApplier = new AsyncDataLogApplier(dummyApplier);
    for (int i = 0; i < 10; i++) {
      PhysicalPlan plan = new InsertRowPlan(new PartialPath(TEST_SG + i), i, new String[0],
          new String[0]);
      PhysicalPlanLog log = new PhysicalPlanLog(plan);
      logsToApply.add(log);
    }
    Log finalLog = new EmptyContentLog();
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
  }
}