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
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;

public class MultiMeasurementMNode extends MeasurementMNode {

  /** measurement's Schema for one aligned timeseries represented by current leaf node */
  private VectorMeasurementSchema schema;

  MultiMeasurementMNode(
      IEntityMNode parent, String measurementName, VectorMeasurementSchema schema, String alias) {
    super(parent, measurementName, alias);
    this.schema = schema;
  }

  @Override
  public VectorMeasurementSchema getSchema() {
    return schema;
  }

  public void setSchema(VectorMeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public int getMeasurementCount() {
    return schema.getSubMeasurementsCount();
  }

  @Override
  public TSDataType getDataType(String measurementId) {
    int index = schema.getSubMeasurementIndex(measurementId);
    return schema.getSubMeasurementsTSDataTypeList().get(index);
  }

  public List<String> getSubMeasurementList() {
    return schema.getSubMeasurementsList();
  }

  @Override
  public boolean isMultiMeasurement() {
    return true;
  }
}
