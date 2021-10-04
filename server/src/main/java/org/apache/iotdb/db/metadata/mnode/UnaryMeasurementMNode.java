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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

/** Represents an MNode which has a Measurement or Sensor attached to it. */
public class UnaryMeasurementMNode extends MeasurementMNode {

  /** measurement's Schema for one timeseries represented by current leaf node */
  private UnaryMeasurementSchema schema;

  UnaryMeasurementMNode(
      IEntityMNode parent, String measurementName, UnaryMeasurementSchema schema, String alias) {
    super(parent, measurementName, alias);
    this.schema = schema;
  }

  @Override
  public UnaryMeasurementSchema getSchema() {
    return schema;
  }

  public void setSchema(UnaryMeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public int getMeasurementCount() {
    return 1;
  }

  @Override
  public TSDataType getDataType(String measurementId) {
    if (name.equals(measurementId)) {
      return schema.getType();
    } else {
      throw new RuntimeException("MeasurementId mismatch in UnaryMeasurementMNode");
    }
  }

  @Override
  public boolean isUnaryMeasurement() {
    return true;
  }
}
