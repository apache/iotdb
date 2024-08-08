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

package org.apache.iotdb.trigger;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LoggerTrigger implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggerTrigger.class);

  @Override
  public boolean fire(Tablet tablet) throws Exception {
    List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
    for (int i = 0, n = measurementSchemaList.size(); i < n; i++) {
      if (measurementSchemaList.get(i).getType().equals(TSDataType.DOUBLE)) {
        double[] values = (double[]) tablet.values[i];
        for (double value : values) {
          if (value > 100) {
            LOGGER.info("Double type, trigger value > 100");
          }
        }
      } else if (measurementSchemaList.get(i).getType().equals(TSDataType.FLOAT)) {
        float[] values = (float[]) tablet.values[i];
        for (float value : values) {
          if (value > 100) {
            LOGGER.info("Float type, trigger value > 100");
          }
        }

      } else if (measurementSchemaList.get(i).getType().equals(TSDataType.INT64)) {
        long[] values = (long[]) tablet.values[i];
        for (long value : values) {
          if (value > -100) {
            LOGGER.info("Int64 type, trigger value > -100");
          }
        }

      } else if (measurementSchemaList.get(i).getType().equals(TSDataType.INT32)) {
        int[] values = (int[]) tablet.values[i];
        for (int value : values) {
          if (value > -100) {
            LOGGER.info("Int32 type, trigger value > -100");
          }
        }
      } else if (measurementSchemaList.get(i).getType().equals(TSDataType.TEXT)) {
        Binary[] values = (Binary[]) tablet.values[i];
        for (Binary ignored : values) {
          LOGGER.info("Text type, trigger Non empty");
        }

      } else if (measurementSchemaList.get(i).getType().equals(TSDataType.BOOLEAN)) {
        boolean[] values = (boolean[]) tablet.values[i];
        for (boolean ignored : values) {
          LOGGER.info("Boolean type, trigger fires");
        }
      }
    }
    return true;
  }
}
