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
package org.apache.iotdb.db.qp.physical.sys;

import java.util.List;

import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class CreateDeviceTemplatePlan extends PhysicalPlan {
  
  private String deviceType;
  private List<MeasurementSchema> schemaList;
  
  public CreateDeviceTemplatePlan(String deviceType, List<MeasurementSchema> schemaList) {
    super(false, OperatorType.CREATE_DEVICE_TEMPLATE);
    this.setDeviceType(deviceType);
    this.setSchemaList(schemaList);
  }

  @Override
  public List<Path> getPaths() {
    return null;
  }

  public String getDeviceType() {
    return deviceType;
  }

  public void setDeviceType(String deviceType) {
    this.deviceType = deviceType;
  }

  public List<MeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public void setSchemaList(List<MeasurementSchema> schemaList) {
    this.schemaList = schemaList;
  }
  
  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (MeasurementSchema schema : schemaList) {
      String ret = String.format(
          "%s%n" + "measurement: %s%n" + "dataType: %s%n" + "encoding: %s%n",
          deviceType, schema.getMeasurementId(), schema.getType(), 
          schema.getEncodingType());
      stringBuilder.append(ret);
    }
    return stringBuilder.toString();
  }

}
