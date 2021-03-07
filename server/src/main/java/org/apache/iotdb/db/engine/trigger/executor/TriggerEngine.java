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

package org.apache.iotdb.db.engine.trigger.executor;

import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;

public class TriggerEngine {

  public static void createTrigger() {}

  public static void dropTrigger() {}

  public static void fire(InsertRowPlan insertRowPlan) throws TriggerExecutionException {
    MeasurementMNode[] mNodes = insertRowPlan.getMeasurementMNodes();
    int size = mNodes.length;

    long timestamp = insertRowPlan.getTime();
    Object[] values = insertRowPlan.getValues();

    for (int i = 0; i < size; ++i) {
      TriggerExecutor executor = mNodes[i].getTriggerExecutor();
      if (executor == null) {
        continue;
      }
      executor.fireIfActivated(timestamp, values[i]);
    }
  }

  public static void fire(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws TriggerExecutionException {
    for (InsertRowPlan insertRowPlan : insertRowsOfOneDevicePlan.getRowPlans()) {
      fire(insertRowPlan);
    }
  }

  public static void fire(InsertTabletPlan insertTabletPlan) throws TriggerExecutionException {
    MeasurementMNode[] mNodes = insertTabletPlan.getMeasurementMNodes();
    int size = mNodes.length;

    long[] timestamps = insertTabletPlan.getTimes();
    Object[] columns = insertTabletPlan.getColumns();

    for (int i = 0; i < size; ++i) {
      TriggerExecutor executor = mNodes[i].getTriggerExecutor();
      if (executor == null) {
        continue;
      }
      executor.fireIfActivated(timestamps, columns[i]);
    }
  }

  public static void deleteTimeseries() {} // todo: delete sg?

  private TriggerEngine() {}
}
