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
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CreatePipeSinkOperator extends Operator {
  private String pipeSinkName;
  private String pipeSinkType;
  private Map<String, String> pipeSinkAttributes;

  public CreatePipeSinkOperator(String pipeSinkName, String pipeSinkType) {
    super(SQLConstant.TOK_CREATE_PIPESINK);
    this.pipeSinkName = pipeSinkName;
    this.pipeSinkType = pipeSinkType;
    pipeSinkAttributes = new HashMap<>();
    this.operatorType = OperatorType.CREATE_PIPESINK;
  }

  public void setPipeSinkAttributes(Map<String, String> pipeSinkAttributes) {
    this.pipeSinkAttributes = pipeSinkAttributes;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    CreatePipeSinkPlan plan = new CreatePipeSinkPlan(pipeSinkName, pipeSinkType);
    Iterator<Map.Entry<String, String>> iterator = pipeSinkAttributes.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      plan.addPipeSinkAttribute(entry.getKey(), entry.getValue());
    }
    return plan;
  }
}
