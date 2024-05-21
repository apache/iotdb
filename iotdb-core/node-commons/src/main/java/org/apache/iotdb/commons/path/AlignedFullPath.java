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

package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.exception.IllegalPathException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public class AlignedFullPath implements IFullPath {

  public static final String VECTOR_PLACEHOLDER = "";

  private final IDeviceID deviceID;

  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;

  public AlignedFullPath(
      IDeviceID deviceID, List<String> measurementList, List<IMeasurementSchema> schemaList) {
    this.deviceID = deviceID;
    this.measurementList = measurementList;
    this.schemaList = schemaList;
  }

  @Override
  public IDeviceID getDeviceId() {
    return deviceID;
  }

  @Override
  public TSDataType getSeriesType() {
    return TSDataType.VECTOR;
  }

  public List<String> getMeasurementList() {
    return measurementList;
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public int getColumnNum() {
    return measurementList.size();
  }

  // Never use this method, this will be removed soon
  @Deprecated
  public PartialPath concatNode(String measurement) {
    try {
      return new PartialPath(deviceID, measurement);
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
  }
}
