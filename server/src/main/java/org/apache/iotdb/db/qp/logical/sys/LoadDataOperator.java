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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

/**
 * this class maintains information in Author statement, including CREATE, DROP, GRANT and REVOKE.
 */
public class LoadDataOperator extends Operator {

  private final String inputFilePath;
  private final String measureType;

  /** Constructor of LoadDataOperator. */
  public LoadDataOperator(int tokenIntType, String inputFilePath, String measureType) {
    super(tokenIntType);
    operatorType = OperatorType.LOAD_DATA;
    this.inputFilePath = inputFilePath;
    this.measureType = measureType;
  }

  public String getInputFilePath() {
    return inputFilePath;
  }

  public String getMeasureType() {
    return measureType;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator) {
    return new LoadDataPlan(inputFilePath, measureType);
  }
}
