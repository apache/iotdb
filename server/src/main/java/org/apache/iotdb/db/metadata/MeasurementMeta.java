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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class MeasurementMeta {
  private MeasurementSchema measurementSchema = null;
  private String alias = null; // TODO get schema by alias
  private TimeValuePair timeValuePair = null;

  public MeasurementMeta(
      MeasurementSchema measurementSchema, String alias, TimeValuePair timeValuePair) {
    this.measurementSchema = measurementSchema;
    this.alias = alias;
    this.timeValuePair = timeValuePair;
  }

  public MeasurementMeta(MeasurementSchema measurementSchema, String alias) {
    this.measurementSchema = measurementSchema;
    this.alias = alias;
  }

  public MeasurementMeta(MeasurementSchema measurementSchema) {
    this.measurementSchema = measurementSchema;
  }

  public MeasurementSchema getMeasurementSchema() {
    return measurementSchema;
  }

  public void setMeasurementSchema(MeasurementSchema measurementSchema) {
    this.measurementSchema = measurementSchema;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public TimeValuePair getTimeValuePair() {
    return timeValuePair;
  }

  public synchronized void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (this.timeValuePair == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        this.timeValuePair =
            new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > this.timeValuePair.getTimestamp()
        || (timeValuePair.getTimestamp() == this.timeValuePair.getTimestamp()
            && highPriorityUpdate)) {
      this.timeValuePair.setTimestamp(timeValuePair.getTimestamp());
      this.timeValuePair.setValue(timeValuePair.getValue());
    }
  }
}
