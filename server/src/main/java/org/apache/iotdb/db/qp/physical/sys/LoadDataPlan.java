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

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LoadDataPlan extends PhysicalPlan {

  private final String inputFilePath;
  private final String measureType;

  /** Constructor of LoadDataPlan. */
  public LoadDataPlan(String inputFilePath, String measureType) {
    super(false, Operator.OperatorType.LOAD_DATA);
    this.inputFilePath = inputFilePath;
    this.measureType = measureType;
  }

  @Override
  public List<PartialPath> getPaths() {
    return measureType != null
        ? Collections.singletonList(new PartialPath(new String[] {measureType}))
        : Collections.emptyList();
  }

  public String getInputFilePath() {
    return inputFilePath;
  }

  public String getMeasureType() {
    return measureType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadDataPlan)) {
      return false;
    }
    LoadDataPlan that = (LoadDataPlan) o;
    return Objects.equals(getInputFilePath(), that.getInputFilePath())
        && Objects.equals(getMeasureType(), that.getMeasureType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getInputFilePath(), getMeasureType());
  }
}
