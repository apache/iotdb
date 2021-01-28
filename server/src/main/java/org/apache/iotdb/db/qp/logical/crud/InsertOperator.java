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
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

/**
 * this class extends {@code RootOperator} and process insert statement.
 */
public class InsertOperator extends SFWOperator {

  private long time;
  private String[] measurementList;
  private String[] valueList;

  public InsertOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.INSERT;
  }

  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize,
      PhysicalGenerator generator) throws QueryProcessException {
    if (getMeasurementList().length != getValueList().length) {
      throw new SQLParserException(
          String.format(
              "the measurementList's size %d is not consistent with the valueList's size %d",
              getMeasurementList().length, getValueList().length));
    }

    return new InsertRowPlan(getSelectedPaths().get(0), getTime(), getMeasurementList(),
        getValueList());
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

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

}
