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

package org.apache.iotdb.db.engine.trigger.sink.local;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Collections;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class LocalIoTDBHandler implements Handler<LocalIoTDBConfiguration, LocalIoTDBEvent> {

  private IPlanExecutor executor;

  private PartialPath device;
  private String[] measurements;
  private TSDataType[] dataTypes;

  @Override
  public void open(LocalIoTDBConfiguration configuration) throws Exception {
    executor = new PlanExecutor();

    device = configuration.getDevice();
    measurements = configuration.getMeasurements();
    dataTypes = configuration.getDataTypes();

    createOrCheckTimeseries();
  }

  private void createOrCheckTimeseries() throws MetadataException, SinkException {
    for (int i = 0; i < measurements.length; ++i) {
      String measurement = measurements[i];
      TSDataType dataType = dataTypes[i];

      PartialPath path = new PartialPath(device.getFullPath(), measurement);
      if (!IoTDB.metaManager.isPathExist(path)) {
        IoTDB.metaManager.createTimeseries(
            path,
            dataType,
            getDefaultEncoding(dataType),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      } else {
        if (!IoTDB.metaManager.getSeriesSchema(device, measurement).getType().equals(dataType)) {
          throw new SinkException(
              String.format("The data type of %s you provided was not correct.", path));
        }
      }
    }
  }

  @Override
  public void onEvent(LocalIoTDBEvent event)
      throws QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    InsertRowPlan plan = new InsertRowPlan();
    plan.setNeedInferType(false);
    plan.setPrefixPath(device);
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
          "Current system mode is read-only, non-query operation is not supported.");
    }
    executor.processNonQuery(plan);
  }
}
