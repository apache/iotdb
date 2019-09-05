/**
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

import org.apache.iotdb.db.qp.logical.ExecutableOperator;

/**
 * this class extends {@code RootOperator} and process insert statement.
 */
public class InsertOperator extends ExecutableOperator {

  private long time;
  private String[] measurementList;
  private String[] valueList;
  private SetPathOperator setPathOperator;


  public InsertOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.INSERT;
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


  @Override
  public SetPathOperator getSetPathOperator() {
    return setPathOperator;
  }

  @Override
  public boolean setSetPathOperator(SetPathOperator setPathOperator) {
    this.setPathOperator = setPathOperator;
    return false;
  }
}
