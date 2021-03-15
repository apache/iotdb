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

package org.apache.iotdb.db.sink.ts;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.sink.api.Sink;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TimeSeriesSink implements Sink<TimeSeriesEvent> {

  private final IPlanExecutor executor;

  private final PartialPath device;
  private final String[] measurements;
  private final TSDataType[] dataTypes;

  public TimeSeriesSink(String device, String[] measurements, TSDataType[] dataTypes)
      throws IllegalPathException, QueryProcessException {
    executor = new PlanExecutor();

    this.device = new PartialPath(device);
    this.measurements = measurements;
    this.dataTypes = dataTypes;
  }

  @Override
  public void onEvent(TimeSeriesEvent event)
      throws QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    InsertRowPlan plan = new InsertRowPlan();
    plan.setNeedInferType(true);
    plan.setDeviceId(device);
    plan.setMeasurements(measurements);
    plan.setDataTypes(dataTypes);
    plan.setTime(event.getTimestamp());
    plan.setValues(event.getValues());
    executeNonQuery(plan);
  }

  private void executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation.");
    }
    executor.processNonQuery(plan);
  }
}
