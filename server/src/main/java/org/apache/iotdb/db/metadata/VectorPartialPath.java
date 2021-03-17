/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;

import java.util.List;

public class VectorPartialPath extends PartialPath {

  private List<PartialPath> subSensorsPathList;

  public VectorPartialPath(String path, List<PartialPath> subSensorsPathList)
      throws IllegalPathException {
    super(path);
    this.subSensorsPathList = subSensorsPathList;
  }

  public VectorPartialPath(String device, String measurement, List<PartialPath> subSensorsPathList)
      throws IllegalPathException {
    super(device, measurement);
    this.subSensorsPathList = subSensorsPathList;
  }

  public VectorPartialPath(String[] partialNodes, List<PartialPath> subSensorsPathList) {
    super(partialNodes);
    this.subSensorsPathList = subSensorsPathList;
  }

  public VectorPartialPath(String path, boolean needSplit, List<PartialPath> subSensorsPathList) {
    super(path, needSplit);
    this.subSensorsPathList = subSensorsPathList;
  }

  public List<PartialPath> getSubSensorsPathList() {
    return subSensorsPathList;
  }

  public void setSubSensorsPathList(List<PartialPath> subSensorsPathList) {
    this.subSensorsPathList = subSensorsPathList;
  }

  public void addSubSensor(PartialPath path) {
    this.subSensorsPathList.add(path);
  }

  @Override
  public String getMeasurement() {
    return subSensorsPathList.get(0).toString();
  }
}
