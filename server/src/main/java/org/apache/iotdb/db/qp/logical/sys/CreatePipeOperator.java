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
 *
 */
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.HashMap;
import java.util.Map;

// TODO: remove this
public class CreatePipeOperator extends Operator {
  private String pipeName;
  private String pipeSinkName;
  private long startTime;
  private Map<String, String> pipeAttributes;

  public CreatePipeOperator(String pipeName, String pipeSinkName) {
    super(SQLConstant.TOK_CREATE_PIPE);
    this.operatorType = OperatorType.CREATE_PIPE;

    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    this.startTime = 0;
    this.pipeAttributes = new HashMap<>();
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setPipeAttributes(Map<String, String> pipeAttributes) {
    this.pipeAttributes = pipeAttributes;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return null;
  }
}
