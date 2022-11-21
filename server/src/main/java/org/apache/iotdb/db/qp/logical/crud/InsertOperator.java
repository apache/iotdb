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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.List;

/** this class extends {@code RootOperator} and process insert statement. */
public class InsertOperator extends Operator {

  private PartialPath device;

  private long[] times;
  private String[] measurementList;
  private List<String[]> valueLists;

  private boolean isAligned;

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

  public List<String[]> getValueLists() {
    return valueLists;
  }

  public void setValueLists(List<String[]> valueLists) {
    this.valueLists = valueLists;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return null;
  }
}
