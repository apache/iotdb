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

package org.apache.iotdb.db.writelog.node;

import java.io.File;
import java.nio.BufferOverflowException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.writelog.io.DifferentialSingleFileLogReader;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DifferentialWriteLogNode extends ExclusiveWriteLogNode {

  private static final Logger logger = LoggerFactory.getLogger(DifferentialWriteLogNode.class);
  // TODO: make WINDOW_LENGTH a config
  public static final int WINDOW_LENGTH = 100;
  private Queue<PhysicalPlan> planWindow;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   */
  public DifferentialWriteLogNode(String identifier) {
    super(identifier);
    planWindow = new ArrayDeque<>(WINDOW_LENGTH);
  }

  @Override
  void putLog(PhysicalPlan plan) {
    logBuffer.mark();
    Pair<PhysicalPlan, Integer> similarPlanIndex = findSimilarPlan(plan);
    try {
      serialize(plan, similarPlanIndex);
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      logBuffer.reset();
      sync();
      serialize(plan, similarPlanIndex);
    }
    bufferedLogNum ++;
    CommonUtils.updatePlanWindow(plan, WINDOW_LENGTH, planWindow);
  }

  @Override
  void nextFileWriter() {
    super.nextFileWriter();
    planWindow.clear();
  }

  private void serialize(PhysicalPlan plan, Pair<PhysicalPlan, Integer> similarPlanIndex) {
    if (similarPlanIndex == null) {
      plan.serialize(logBuffer);
    } else {
      serializeDifferentially(plan, similarPlanIndex);
    }
  }

  private void serializeNonDifferentially(PhysicalPlan plan) {
    logBuffer.putInt(-1);
    plan.serialize(logBuffer);
  }

  private void serializeDifferentially(PhysicalPlan plan,
      Pair<PhysicalPlan, Integer> similarPlanIndex) {
    if (plan instanceof InsertPlan) {
      serializeDifferentially(((InsertPlan) plan), ((InsertPlan) similarPlanIndex.left),
          similarPlanIndex.right);
    } else {
      serializeNonDifferentially(plan);
    }
  }

  private void serializeDifferentially(InsertPlan plan, InsertPlan base, int index) {
    plan.serialize(logBuffer, base, index);
  }

  private Pair<PhysicalPlan, Integer> findSimilarPlan(PhysicalPlan plan) {
    int index = -1;
    for (PhysicalPlan next : planWindow) {
      index++;
      if (isPlanSimilarEnough(plan, next)) {
        return new Pair<>(plan, index);
      }
    }
    return null;
  }

  private boolean isPlanSimilarEnough(PhysicalPlan planA, PhysicalPlan planB) {
    if (!planA.getClass().equals(planB.getClass())) {
      return false;
    }
    if (planA instanceof InsertTabletPlan && planB instanceof InsertTabletPlan) {
      return isPlanSimilarEnough(((InsertTabletPlan) planA), ((InsertTabletPlan) planB));
    }
    return false;
  }

  private boolean isPlanSimilarEnough(InsertPlan planA, InsertPlan planB) {
    return planA.getDeviceId().equals(planB.getDeviceId()) &&
        Arrays.equals(planA.getMeasurements(), planB.getMeasurements()) &&
        Arrays.equals(planA.getDataTypes(), planB.getDataTypes());
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(logDirectory).listFiles();
    Arrays.sort(logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(WAL_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles, DifferentialSingleFileLogReader::new);
  }
}
