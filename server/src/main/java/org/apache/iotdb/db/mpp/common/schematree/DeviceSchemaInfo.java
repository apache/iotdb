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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;

public class DeviceSchemaInfo {

  private PartialPath devicePath;
  private boolean isAligned;
  private List<MeasurementSchema> measurementSchemaList;

  public DeviceSchemaInfo(
      PartialPath devicePath, boolean isAligned, List<MeasurementSchema> measurementSchemaList) {
    this.devicePath = devicePath;
    this.isAligned = isAligned;
    this.measurementSchemaList = measurementSchemaList;
  }

  public List<MeasurementSchema> getMeasurementSchemaList() {
    return measurementSchemaList;
  }

  public boolean isAligned() {
    return isAligned;
  }
}
