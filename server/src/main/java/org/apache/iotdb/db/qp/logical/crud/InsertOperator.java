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
package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.Arrays;

/** this class extends {@code RootOperator} and process insert statement. */
public class InsertOperator extends Operator {

  private PartialPath device;

  private long[] times;
  private String[] measurementList;
  private String[] valueList;

  public InsertOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.INSERT;
  }

  public PartialPath getDevice() {
    return device;
  }

  public void setDevice(PartialPath device) {
    this.device = device;
  }

  public String[] getMeasurementList() {
    return measurementList;
  }

  public void setMeasurementList(String[] measurementList) {
    this.measurementList = measurementList;
  }

  public String[] getValueList() {
    return valueList;
  }

  public void setValueList(String[] insertValue) {
    this.valueList = insertValue;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    int measurementsNum = 0;
    for (String measurement : measurementList) {
      if (measurement.startsWith("(") && measurement.endsWith(")")) {
        measurementsNum += measurement.replace("(", "").replace(")", "").split(",").length;
      } else {
        measurementsNum++;
      }
    }
    if (measurementsNum == 0 || (valueList.length % measurementsNum != 0)) {
      throw new SQLParserException(
          String.format(
              "the measurementList's size %d is not consistent with the valueList's size %d",
              measurementsNum, valueList.length));
    }
    if (measurementsNum == valueList.length) {
      return new InsertRowPlan(device, times[0], measurementList, valueList);
    }
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < times.length; i++) {
      insertRowsPlan.addOneInsertRowPlan(
          new InsertRowPlan(
              device,
              times[i],
              measurementList,
              Arrays.copyOfRange(valueList, i * measurementsNum, (i + 1) * measurementsNum)),
          i);
    }
    return insertRowsPlan;
  }
}
